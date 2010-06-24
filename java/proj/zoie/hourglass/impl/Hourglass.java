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
import proj.zoie.api.ZoieVersion;

public class Hourglass<R extends IndexReader, D, V extends ZoieVersion> implements IndexReaderFactory<ZoieIndexReader<R>>, DataConsumer<D,V>
{
  public static final Logger log = Logger.getLogger(Hourglass.class);
  private final HourglassDirectoryManagerFactory<V> _dirMgrFactory;
  private final ZoieIndexableInterpreter<D> _interpreter;
  private final IndexReaderDecorator<R> _decorator;
  private final ZoieConfig<V> _zConfig;
  private volatile ZoieSystem<R, D,V> _currentZoie;
  private volatile boolean _isShutdown = false;
  private final ReaderManager<R, D,V> _readerMgr;
  private volatile V _currentVersion = null;
  public Hourglass(HourglassDirectoryManagerFactory<V> dirMgrFactory, ZoieIndexableInterpreter<D> interpreter, IndexReaderDecorator<R> readerDecorator,ZoieConfig<V> zoieConfig)
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
    _readerMgr = new ReaderManager<R, D,V>(this, _dirMgrFactory, _decorator, loadArchives());
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
  private ZoieSystem<R, D,V> createZoie(DirectoryManager<V> dirmgr)
  {
    return new ZoieSystem<R, D,V>(dirmgr, _interpreter, _decorator, _zConfig);
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
    if (_isShutdown)
    {
      log.warn("System already shut down. No search request allowed.");
      return list;// if already shutdown, return an empty list
    }
    return _readerMgr.getIndexReaders();
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
  public synchronized void consume(Collection<DataEvent<D,V>> data) throws ZoieException
  {
    if (data == null || data.size() == 0) return;
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
  }

  public synchronized void shutdown()
  {
    if (_isShutdown)
    {
      log.info("system already shut down");
      return;
    }
    _isShutdown = true;
    _readerMgr.shutdown();
    log.info("shut down complete.");
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#getVersion()
   */
  public V getVersion()
  {
    //_currentVersion = Math.max(_currentVersion, _currentZoie.getCurrentVersion());
    if(_currentZoie.getCurrentVersion() != null)
    {
      if(_currentVersion == null)
      {
        return _currentVersion = _currentZoie.getCurrentVersion();
      }
      else
      {
        _currentVersion = _currentZoie.getCurrentVersion().compareTo(_currentVersion) < 0 ? _currentVersion : _currentZoie.getCurrentVersion();
      }
    }
      
    return _currentVersion;
  }
  public static class ReaderManager<R extends IndexReader, D, V extends ZoieVersion>
  {
    private final HourglassDirectoryManagerFactory<V> _dirMgrFactory;
    private final Hourglass<R, D,V> hg;
    private final IndexReaderDecorator<R> _decorator;
    private volatile Box<R, D,V> box;
    private ExecutorService threadPool = Executors.newCachedThreadPool();
    public ReaderManager(Hourglass<R, D,V> hourglass, HourglassDirectoryManagerFactory<V> dirMgrFactory,
        IndexReaderDecorator<R> decorator,
        List<ZoieIndexReader<R>> initArchives)
    {
      hg = hourglass;
      _dirMgrFactory = dirMgrFactory;
      _decorator = decorator;
      box = new Box<R, D,V>(initArchives, Collections.EMPTY_LIST, Collections.EMPTY_LIST, _decorator);
    }
    public synchronized ZoieSystem<R, D,V> retireAndNew(final ZoieSystem<R, D,V> old)
    {
      DirectoryManager<V> _dirMgr = _dirMgrFactory.getDirectoryManager();
      _dirMgrFactory.clearRecentlyChanged();
      ZoieSystem<R, D,V> newzoie = hg.createZoie(_dirMgr);
      List<ZoieSystem<R, D,V>> actives = new LinkedList<ZoieSystem<R, D,V>>(box._actives);
      List<ZoieSystem<R, D,V>> retiring = new LinkedList<ZoieSystem<R, D,V>>(box._retiree);
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
      Box<R, D,V> newbox = new Box<R, D,V>(box._archives, retiring, actives, _decorator);
      box = newbox;
      return newzoie;
    }
    /**
     * @param zoie
     * @param reader the IndexReader opened on the index the give zoie had written to.
     */
    public synchronized void archive(ZoieSystem<R, D,V> zoie, IndexReader reader)
    {
      List<ZoieIndexReader<R>> _archives = new LinkedList<ZoieIndexReader<R>>(box._archives);
      List<ZoieSystem<R, D,V>> actives = new LinkedList<ZoieSystem<R, D,V>>(box._actives);
      List<ZoieSystem<R, D,V>> retiring = new LinkedList<ZoieSystem<R, D,V>>(box._retiree);
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
      Box<R, D,V> newbox = new Box<R, D,V>(_archives, retiring, actives, _decorator);
      box = newbox;
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
    public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
    {
      List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
      synchronized(this)
      {
        if (box == null) return list;
        // add the archived index readers
        for(ZoieIndexReader<R> r : box._archives)
        {
          r.incRef();
          list.add(r);
        }
        // add the retiring index readers
        for(ZoieSystem<R, D,V> zoie : box._retiree)
        {
          list.addAll(zoie.getIndexReaders());
        }
        // add the active index readers
        for(ZoieSystem<R, D,V> zoie : box._actives)
        {
          list.addAll(zoie.getIndexReaders());
        }
      }
      return list;
    }  
    protected void retire(ZoieSystem<R, D,V> zoie)
    {
      while(true)
      {
        try
        {
          zoie.flushEvents(200000L);
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
            break;
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
      zoie.shutdown();
    }
    private IndexReader getArchive(ZoieSystem<R, D,V> zoie) throws CorruptIndexException, IOException
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
  public static class Box<R extends IndexReader, D, V extends ZoieVersion>
  {
    List<ZoieIndexReader<R>> _archives;
    List<ZoieSystem<R, D, V>> _retiree;
    List<ZoieSystem<R, D, V>> _actives;
    IndexReaderDecorator<R> _decorator;
    /**
     * Copy the given lists to have immutable behavior.
     * @param archives
     * @param retiree
     * @param actives
     * @param decorator
     */
    public Box(List<ZoieIndexReader<R>> archives, List<ZoieSystem<R, D, V>> retiree, List<ZoieSystem<R, D,V>> actives, IndexReaderDecorator<R> decorator)
    {
      _archives = new LinkedList<ZoieIndexReader<R>>(archives);
      _retiree = new LinkedList<ZoieSystem<R, D, V>>(retiree);
      _actives = new LinkedList<ZoieSystem<R, D, V>>(actives);
      _decorator = decorator;
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
      for(ZoieSystem<R, D, V> zoie : _retiree)
      {
        zoie.shutdown();
      }
      // add the active index readers
      for(ZoieSystem<R, D, V> zoie : _actives)
      {
        zoie.shutdown();
      }
    }
  }
}
