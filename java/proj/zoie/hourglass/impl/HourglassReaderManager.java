package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.impl.indexing.internal.ZoieIndexDeletionPolicy;

public class HourglassReaderManager<R extends IndexReader, V>
{
  public static final Logger log = Logger.getLogger(HourglassReaderManager.class.getName());
  private final HourglassDirectoryManagerFactory _dirMgrFactory;
  private final Hourglass<R, V> hg;
  private final IndexReaderDecorator<R> _decorator;
  private volatile Box<R, V> box;
  private volatile boolean isShutdown = false;
  private ExecutorService threadPool = Executors.newCachedThreadPool();
  public HourglassReaderManager(final Hourglass<R, V> hourglass, HourglassDirectoryManagerFactory dirMgrFactory,
      IndexReaderDecorator<R> decorator,
      List<ZoieIndexReader<R>> initArchives)
  {
    hg = hourglass;
    _dirMgrFactory = dirMgrFactory;
    _decorator = decorator;
    box = new Box<R, V>(initArchives, Collections.EMPTY_LIST, Collections.EMPTY_LIST, _decorator);
    threadPool.execute(new Runnable(){
      final int trimThreshold = hourglass._scheduler.getTrimThreshold();

      @Override
      public void run()
      {
        while(true)
        {
          try
          {
            Thread.sleep(60000);
          } catch (InterruptedException e)
          {
            log.warn(e);
          }
          List<ZoieIndexReader<R>> archives = new LinkedList<ZoieIndexReader<R>>(box._archives);
          List<ZoieIndexReader<R>> add = new LinkedList<ZoieIndexReader<R>>();
          try
          {
            hourglass._shutdownLock.readLock().lock();
            if (isShutdown)
            {
              log.info("Already shut down. Quiting maintenance thread.");
              break;
            }
            if (archives.size() > trimThreshold)
            { 
              log.info("to maintain");
            } else continue;
//            consolidate(archives, add);
            trim(archives);
            // swap the archive with consolidated one
            swapArchives(archives, add);
          } finally
          {
            hourglass._shutdownLock.readLock().unlock();
          }
        }
      }});
  }
  /**
   * consolidate the archived Index to one big optimized index and put in add
   * @param toRemove
   * @param add
   */
  private void trim(List<ZoieIndexReader<R>> toRemove)
  {
    long timenow = System.currentTimeMillis();
    List<ZoieIndexReader<R>> toKeep = new LinkedList<ZoieIndexReader<R>>();
    Calendar now = Calendar.getInstance();
    now.setTimeInMillis(timenow);
    Calendar threshold = hg._scheduler.getTrimTime(now);
    for(int i=0; i<toRemove.size(); i++)
    {
      SimpleFSDirectory dir = (SimpleFSDirectory) toRemove.get(i).directory();
      String path = dir.getFile().getName();
      Calendar archivetime = null;
      try
      {
        archivetime = HourglassDirectoryManagerFactory.getCalendarTime(path);
      } catch (ParseException e)
      {
        log.error("index directory name bad. potential corruption. Move on without trimming.", e);
        toKeep.add(toRemove.get(i));
        continue;
      }
      if (archivetime.before(threshold))
      {
        log.info("trimming: remove " + path);
        log.info(dir.getFile() + " -before--" + (dir.getFile().exists()?" not deleted ":" deleted"));
        FileUtil.rmDir(dir.getFile());
        log.info(dir.getFile() + " -after--" + (dir.getFile().exists()?" not deleted ":" deleted"));
      } else
      {
        toKeep.add(toRemove.get(i));
      }
    }
    toRemove.removeAll(toKeep);
  }
  /**
   * consolidate the archived Index to one big optimized index and put in add
   * @param archived
   * @param add
   */
  private void consolidate(List<ZoieIndexReader<R>> archived, List<ZoieIndexReader<R>> add)
  {
    log.info("begin consolidate ... ");
    long b4 = System.currentTimeMillis();
    SimpleFSDirectory target = (SimpleFSDirectory) archived.get(0).directory();
    log.info("into: "+target.getFile().getAbsolutePath());
    SimpleFSDirectory sources[] = new SimpleFSDirectory[archived.size()-1];
    IndexSignature sigs[] = new IndexSignature[archived.size()];
    sigs[0] = _dirMgrFactory.getIndexSignature(target.getFile()); // the target index signature
    log.info("target version: " + sigs[0].getVersion());
    for(int i=1; i<archived.size(); i++)
    {
      sources[i-1] = (SimpleFSDirectory) archived.get(i).directory();
      sigs[i] = _dirMgrFactory.getIndexSignature(sources[i-1].getFile());  // get other index signatures
      log.info("from: " + sources[i-1].getFile().getAbsolutePath());
    }
    IndexWriter idxWriter = null;
    try
    {
      idxWriter = new IndexWriter(target, null, false, new ZoieIndexDeletionPolicy(), MaxFieldLength.UNLIMITED);
      idxWriter.addIndexesNoOptimize(sources);
      idxWriter.optimize(1);
    } catch (CorruptIndexException e)
    {
      ZoieHealth.setFatal();
      log.error("index currupted during consolidation", e);
    } catch (LockObtainFailedException e)
    {
      ZoieHealth.setFatal();
      log.error("LockObtainFailedException during consolidation", e);
    } catch (IOException e)
    {
      ZoieHealth.setFatal();
      log.error("IOException during consolidation", e);
    } finally
    {
      if (idxWriter != null)
      {
        try
        {
          idxWriter.close();
          // remove the originals from disk
          for(SimpleFSDirectory dir : sources)
          {
            IndexSignature sig = _dirMgrFactory.getIndexSignature(dir.getFile());
            log.info(dir.getFile() + "---" + (dir.getFile().exists()?" not deleted ":" deleted") + " version: " + sig.getVersion());
            FileUtil.rmDir(dir.getFile());
            log.info(dir.getFile() + "---" + (dir.getFile().exists()?" not deleted ":" deleted"));
          }
          long tgtversion = 0;
          for(int i = sigs.length - 1; i >= 0; i--)
          { // get the largest version so far
            if (sigs[i].getVersion() > tgtversion) tgtversion = sigs[i].getVersion();
          }
          // save the version to target
          IndexSignature tgtsig = _dirMgrFactory.getIndexSignature(target.getFile());
          tgtsig.updateVersion(tgtversion);
          _dirMgrFactory.saveIndexSignature(target.getFile(), tgtsig);
          log.info("saveIndexSignature to " + target.getFile().getAbsolutePath() + " at version: " + tgtsig.getVersion());
          // open index reader for the consolidated index
          IndexReader reader = IndexReader.open(target, true);
          // decorate the index
          ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);
          add.add(zoiereader);
          long b5 = System.currentTimeMillis();
          log.info("done consolidate in " + (System.currentTimeMillis() - b4)+"ms  blocked for " + (System.currentTimeMillis()-b5));
        } catch (CorruptIndexException e)
        {
          ZoieHealth.setFatal();
          log.error("index currupted during consolidation", e);
        } catch (IOException e)
        {
          ZoieHealth.setFatal();
          log.error("IOException during consolidation", e);
        }
      }
    }
  }
  /**
   * The readers removed will also be decRef(). But the readers to be added will NOT get incRef(),
   * which means we assume the newly added ones have already been incRef().
   * remove and add should be <b>disjoint</b>
   * @param remove the readers to be remove. This has to be disjoint from add.
   * @param add
   */
  public synchronized void swapArchives(List<ZoieIndexReader<R>> remove, List<ZoieIndexReader<R>> add)
  {
    List<ZoieIndexReader<R>> archives = new LinkedList<ZoieIndexReader<R>>(add);
    if (!box._archives.containsAll(remove))
    {
      log.error("swapArchives: potential sync issue. ");
    }
    archives.addAll(box._archives);
    archives.removeAll(remove);
    for(ZoieIndexReader<R> r : remove)
    {
      r.decZoieRef();
      if (log.isDebugEnabled())
      {
        log.debug("remove time " + r.directory() + " refCount: " + r.getRefCount());
      }
    }
    Box<R, V> newbox = new Box<R, V>(archives, box._retiree, box._actives, _decorator);
    box = newbox;
  }
  public synchronized ZoieSystem<R, V> retireAndNew(final ZoieSystem<R, V> old)
  {
    DirectoryManager _dirMgr = _dirMgrFactory.getDirectoryManager();
    _dirMgrFactory.clearRecentlyChanged();
    ZoieSystem<R, V> newzoie = hg.createZoie(_dirMgr);
    List<ZoieSystem<R, V>> actives = new LinkedList<ZoieSystem<R, V>>(box._actives);
    List<ZoieSystem<R, V>> retiring = new LinkedList<ZoieSystem<R, V>>(box._retiree);
    if (old!=null)
    {
      actives.remove(old);
      retiring.add(old);
      threadPool.execute(new Runnable()
      {
        @Override
        public void run()
        {
          retire(old);
        }});
    }
    actives.add(newzoie);
    Box<R, V> newbox = new Box<R, V>(box._archives, retiring, actives, _decorator);
    box = newbox;
    return newzoie;
  }
  /**
   * @param zoie
   * @param reader the IndexReader opened on the index the give zoie had written to.
   */
  public synchronized void archive(ZoieSystem<R, V> zoie, IndexReader reader)
  {
    List<ZoieIndexReader<R>> _archives = new LinkedList<ZoieIndexReader<R>>(box._archives);
    List<ZoieSystem<R, V>> actives = new LinkedList<ZoieSystem<R, V>>(box._actives);
    List<ZoieSystem<R, V>> retiring = new LinkedList<ZoieSystem<R, V>>(box._retiree);
    retiring.remove(zoie);
    if (reader != null)
    {
      try
      {
        ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);
        _archives.add(zoiereader);
      } catch (IOException e)
      {
        log.error(e);
      }
    }
    Box<R, V> newbox = new Box<R, V>(_archives, retiring, actives, _decorator);
    box = newbox;
  }
  private synchronized void preshutdown()
  {
    log.info("shutting down thread pool.");
    threadPool.shutdown();
    isShutdown = true;
  }
  public void shutdown()
  {
    preshutdown();
    while(true)
    {
      TimeUnit unit=TimeUnit.SECONDS;
      long t=10L;
      try
      {
        if (threadPool.awaitTermination(t, unit)) break;
      } catch (InterruptedException e)
      {
        log.warn("Exception when trying to shutdown. Will retry.", e);
      }
    }
    log.info("shutting down thread pool complete.");
    log.info("shutting down indices.");
    box.shutdown();
    log.info("shutting down indices complete.");
  }
  public synchronized List<ZoieIndexReader<R>> getIndexReaders() throws IOException
  {
    List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
    // add the archived index readers
    for(ZoieIndexReader<R> r : box._archives)
    {
      r.incZoieRef();
      list.add(r);
    }
    // add the retiring index readers
    for(ZoieSystem<R, V> zoie : box._retiree)
    {
      list.addAll(zoie.getIndexReaders());
    }
    // add the active index readers
    for(ZoieSystem<R, V> zoie : box._actives)
    {
      list.addAll(zoie.getIndexReaders());
    }
    return list;
  }  
  protected void retire(ZoieSystem<R, V> zoie)
  {
    long t0 = System.currentTimeMillis();
    log.info("retiring " + zoie.getAdminMBean().getIndexDir());
    while(true)
    {
      long flushwait = 200000L;
      try
      {
        zoie.flushEvents(flushwait);
        zoie.getAdminMBean().setUseCompoundFile(true);
        zoie.getAdminMBean().optimize(1);
        break;
      } catch (IOException e)
      {
        log.error("retiring " + zoie.getAdminMBean().getIndexDir() + " Should investigate. But move on now.", e);
        break;
      } catch (ZoieException e)
      {
        if (e.getMessage().indexOf("timed out")<0)
        {
          break;
        } else
        {
          log.info("retiring " + zoie.getAdminMBean().getIndexDir() + " flushing processing " + flushwait +"ms elapsed");
        }
      }
    }
    IndexReader reader = null;
    try
    {
      reader = getArchive(zoie);
    } catch (CorruptIndexException e)
    {
      log.error("retiring " + zoie.getAdminMBean().getIndexDir() + " Should investigate. But move on now.", e);
    } catch (IOException e)
    {
      log.error("retiring " + zoie.getAdminMBean().getIndexDir() + " Should investigate. But move on now.", e);
    }
    archive(zoie, reader);
    log.info("retired " + zoie.getAdminMBean().getIndexDir() + " in " + (System.currentTimeMillis()-t0)+"ms");
    log.info("Disk Index Size Total Now: " + (hg.getSizeBytes()/1024L) + "KB");
    zoie.shutdown();
  }
  private IndexReader getArchive(ZoieSystem<R, V> zoie) throws CorruptIndexException, IOException
  {
    String dirName = zoie.getAdminMBean().getIndexDir();
    Directory dir = new SimpleFSDirectory(new File(dirName));
    IndexReader reader = null;
    if (IndexReader.indexExists(dir))
    {
      reader  = IndexReader.open(dir, true);
    }
    else
    {
      log.info("empty index " + dirName);
      reader = null;
    }
    return reader;
  }
}
