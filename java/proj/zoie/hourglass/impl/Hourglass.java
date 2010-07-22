package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

public class Hourglass<R extends IndexReader, V> implements IndexReaderFactory<ZoieIndexReader<R>>, DataConsumer<V>
{
  public static final Logger log = Logger.getLogger(Hourglass.class);
  private final HourglassDirectoryManagerFactory _dirMgrFactory;
  private final ZoieIndexableInterpreter<V> _interpreter;
  private final IndexReaderDecorator<R> _decorator;
  private final ZoieConfig _zConfig;
  private volatile ZoieSystem<R, V> _currentZoie;
  private volatile boolean _isShutdown = false;
  private final ReentrantReadWriteLock _shutdownLock = new ReentrantReadWriteLock();
  private final ReaderManager<R, V> _readerMgr;
  private volatile long _currentVersion = 0L;
  public Hourglass(HourglassDirectoryManagerFactory dirMgrFactory, ZoieIndexableInterpreter<V> interpreter, IndexReaderDecorator<R> readerDecorator,ZoieConfig zoieConfig)
  {
    _zConfig = zoieConfig;
    // The following line is a HACK to get it to work with Zoie and save memory
    // used for DocIDMapper.
    // Zoie uses DocIDMapper to convert from UID to doc ID in delete handling.
    // Hourglass assumes that the index is append only (no deletes, no updates)
    // and never needs to handle deletion.
    _zConfig.setDocidMapperFactory(NullDocIDMapperFactory.INSTANCE);

    _dirMgrFactory = dirMgrFactory;
    _dirMgrFactory.clearRecentlyChanged();
    _interpreter = interpreter;
    _decorator = readerDecorator;
    _readerMgr = new ReaderManager<R, V>(this, _dirMgrFactory, _decorator, loadArchives());
    _currentVersion = _dirMgrFactory.getArchivedVersion();
    _currentZoie = _readerMgr.retireAndNew(null);
    _currentZoie.start();
    log.info("start Hourglass at version: " + _currentVersion);
  }
  protected List<ZoieIndexReader<R>> loadArchives()
  {
    List<ZoieIndexReader<R>> archives = new ArrayList<ZoieIndexReader<R>>();
    long t0 = System.currentTimeMillis();
    List<Directory> dirs = _dirMgrFactory.getAllArchivedDirectories();
    for(Directory dir : dirs)
    {
      IndexReader reader;
      try
      {
        reader = IndexReader.open(dir,true);
        ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);
        archives.add(zoiereader);
      } catch (CorruptIndexException e)
      {
        log.error("corruptedIndex", e);
      } catch (IOException e)
      {
        log.error("IOException", e);
      }
    }
    log.info("load "+dirs.size()+" archived indices in " + (System.currentTimeMillis() - t0) + "ms");
    return archives;
  }
  private ZoieSystem<R, V> createZoie(DirectoryManager dirmgr)
  {
    return new ZoieSystem<R, V>(dirmgr, _interpreter, _decorator, _zConfig);
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.IndexReaderFactory#getAnalyzer()
   */
  public Analyzer getAnalyzer()
  {
    return _zConfig.getAnalyzer();
  }

  /**
   * return a list of ZoieIndexReaders. These readers are reference counted and this method
   * should be used in pair with returnIndexReaders(List<ZoieIndexReader<R>> readers) {@link #returnIndexReaders(List)}.
   * It is typical that we create a MultiReader from these readers. When creating MultiReader, it should be created with
   * the closeSubReaders parameter set to false in order to do reference counting correctly.
   * <br> If this indexing system is already shut down, then we return an empty list.
   * @see proj.zoie.hourglass.impl.Hourglass#returnIndexReaders(List)
   * @see proj.zoie.api.IndexReaderFactory#getIndexReaders()
   */
  public synchronized List<ZoieIndexReader<R>> getIndexReaders() throws IOException
  {
    List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
    try
    {
      _shutdownLock.readLock().lock();
      if (_isShutdown)
      {
        log.warn("System already shut down. No search request allowed.");
        return list;// if already shutdown, return an empty list
      }
      return _readerMgr.getBox().getIndexReaders();
    } finally
    {
      _shutdownLock.readLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.IndexReaderFactory#returnIndexReaders(java.util.List)
   */
  public void returnIndexReaders(List<ZoieIndexReader<R>> readers)
  {
    _currentZoie.returnIndexReaders(readers);
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#consume(java.util.Collection)
   */
  public synchronized void consume(Collection<DataEvent<V>> data)
      throws ZoieException
  {
    if (data == null || data.size() == 0) return;
    try
    {
      _shutdownLock.readLock().lock();
      if (_isShutdown)
      {
        log.warn("System already shut down. Rejects indexing request.");
        return; // if the system is already shut down, we don't do anything.
      }
      // TODO  need to check time boundary. When we hit boundary, we need to trigger DM to 
      // use new dir for zoie and the old one will be archive.
      if (!_dirMgrFactory.updateDirectoryManager())
      {
        _currentZoie.consume(data);
      } else
      {
        // new time period
        _currentZoie = _readerMgr.retireAndNew(_currentZoie);
        _currentZoie.start();
        _currentZoie.consume(data);
      }
    } finally
    {
      _shutdownLock.readLock().unlock();
    }
  }
  
  public synchronized void shutdown()
  {
    try
    {
      _shutdownLock.writeLock().lock();
      if (_isShutdown)
      {
        log.info("system already shut down");
        return;
      }
      _isShutdown = true;
    } finally
    {
      _shutdownLock.writeLock().unlock();
    }
    _readerMgr.shutdown();
    log.info("shut down complete.");
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#getVersion()
   */
  public long getVersion()
  {
    _currentVersion = Math.max(_currentVersion, _currentZoie.getCurrentVersion());
    return _currentVersion;
  }
  public static class ReaderManager<R extends IndexReader, V>
  {
    private final HourglassDirectoryManagerFactory _dirMgrFactory;
    private final Hourglass<R, V> hg;
    private final IndexReaderDecorator<R> _decorator;
    private volatile Box<R, V> box;
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    public ReaderManager(Hourglass<R, V> hourglass, HourglassDirectoryManagerFactory dirMgrFactory,
        IndexReaderDecorator<R> decorator,
        List<ZoieIndexReader<R>> initArchives)
    {
      hg = hourglass;
      _dirMgrFactory = dirMgrFactory;
      _decorator = decorator;
      box = new Box<R, V>(initArchives, Collections.EMPTY_LIST, Collections.EMPTY_LIST, _decorator);
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
      ZoieMultiReader<R> zoiereader;
      try
      {
        zoiereader = new ZoieMultiReader<R>(reader, _decorator);
        _archives.add(zoiereader);
        Box<R, V> newbox = new Box<R, V>(_archives, retiring, actives, _decorator);
        box = newbox;
      } catch (IOException e)
      {
        log.error(e);
      }
    }
    public synchronized void shutdown()
    {
      log.info("shutting down thread pool.");
      threadPool.shutdown();
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
    public Box<R, V> getBox()
    {
      return box;
    }
    protected void retire(ZoieSystem<R, V> zoie)
    {
      while(true)
      {
        try
        {
          zoie.flushEvents(200000L);
          break;
        } catch (ZoieException e)
        {
          if (e.getMessage().indexOf("timed out")<0)
            break;
        }
      }
      try
      {
        IndexReader reader = getArchive(zoie);
        archive(zoie, reader);
        zoie.shutdown();
      } catch (CorruptIndexException e)
      {
        log.error("retiring " + zoie.getAdminMBean().getIndexDir() + " ", e);
      } catch (IOException e)
      {
        log.error("retiring " + zoie.getAdminMBean().getIndexDir() + " ", e);
      }
    }
    private IndexReader getArchive(ZoieSystem<R, V> zoie) throws CorruptIndexException, IOException
    {
      String dirName = zoie.getAdminMBean().getIndexDir();
      IndexReader reader = IndexReader.open(new SimpleFSDirectory(new File(dirName)),true);
      return reader;
    }
  }
  public static class Box<R extends IndexReader, V>
  {
    List<ZoieIndexReader<R>> _archives;
    List<ZoieSystem<R, V>> _retiree;
    List<ZoieSystem<R, V>> _actives;
    IndexReaderDecorator<R> _decorator;
    /**
     * Copy the given lists to have immutable behavior.
     * @param archives
     * @param retiree
     * @param actives
     * @param decorator
     */
    public Box(List<ZoieIndexReader<R>> archives, List<ZoieSystem<R, V>> retiree, List<ZoieSystem<R, V>> actives, IndexReaderDecorator<R> decorator)
    {
      _archives = new LinkedList<ZoieIndexReader<R>>(archives);
      _retiree = new LinkedList<ZoieSystem<R, V>>(retiree);
      _actives = new LinkedList<ZoieSystem<R, V>>(actives);
      _decorator = decorator;
    }
    public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
    {
      List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
      // add the archived index readers
      for(ZoieIndexReader<R> r : _archives)
      {
        r.incRef();
        list.add(r);
      }
      // add the retiring index readers
      for(ZoieSystem<R, V> zoie : _retiree)
      {
        list.addAll(zoie.getIndexReaders());
      }
      // add the active index readers
      for(ZoieSystem<R, V> zoie : _actives)
      {
        list.addAll(zoie.getIndexReaders());
      }
      return list;
    }  
    public void shutdown()
    {
      for(ZoieIndexReader<R> r : _archives)
      {
        try
        {
          r.decRef();
        } catch (IOException e)
        {
          log.error("error decRef during shutdown", e);
        }
        log.info("refCount at shutdown: " + r.getRefCount());
      }
      for(ZoieSystem<R, V> zoie : _retiree)
      {
        zoie.shutdown();
      }
      // add the active index readers
      for(ZoieSystem<R, V> zoie : _actives)
      {
        zoie.shutdown();
      }
    }
  }
}
