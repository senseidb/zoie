package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
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
  private DirectoryManager _dirMgr;
  private final ZoieIndexableInterpreter<V> _interpreter;
  private final IndexReaderDecorator<R> _decorator;
  private final ZoieConfig _zConfig;
  private volatile ZoieSystem<R, V> _currentZoie;
  private final LinkedList<ZoieSystem<R, V>> _oldZoies = new LinkedList<ZoieSystem<R,V>>();
  private final List<ZoieIndexReader<R>> archiveList = new ArrayList<ZoieIndexReader<R>>();
  private volatile boolean _isShutdown = false;
  private final ReentrantReadWriteLock _shutdownLock = new ReentrantReadWriteLock();
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
    _dirMgr = _dirMgrFactory.getDirectoryManager();
    _dirMgrFactory.clearRecentlyChanged();
    _interpreter = interpreter;
    _decorator = readerDecorator;
    _currentVersion = loadArchives();
    _currentZoie = createZoie(_dirMgr);
    _currentZoie.start();
    log.info("start at version: " + _currentVersion);
  }
  private long loadArchives()
  {
    long t0 = System.currentTimeMillis();
    List<Directory> dirs = _dirMgrFactory.getAllArchivedDirectories();
    for(Directory dir : dirs)
    {
      IndexReader reader;
      try
      {
        reader = IndexReader.open(dir,true);
        ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);
        archiveList.add(zoiereader);
      } catch (CorruptIndexException e)
      {
        log.error("corruptedIndex", e);
      } catch (IOException e)
      {
        log.error("IOException", e);
      }
    }
    log.info("load "+dirs.size()+" archive Indices in " + (System.currentTimeMillis() - t0) + "ms");
    return _dirMgrFactory.getArchivedVersion();
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
  public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
  {
    List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
    try
    {
      _shutdownLock.readLock().lock();
      if (_isShutdown)
      {
        return list;// if already shutdown, return an empty list
      }
      // add the archived index readers
      for(ZoieIndexReader<R> r : archiveList)
      {
        r.incRef();
        list.add(r);
      }
      if (_oldZoies.size()!=0)
      {
        log.info("number of old zoie now: " + _oldZoies.size());
        for(ZoieSystem<R, V> oldZoie : _oldZoies)
        {
          if(oldZoie.getCurrentBatchSize()+oldZoie.getCurrentDiskBatchSize()+oldZoie.getCurrentMemBatchSize()==0)
          {
            // all events on disk.
            log.info("shutting down ... " + oldZoie.getAdminMBean().getIndexDir());
            oldZoie.shutdown();
            String dirName = oldZoie.getAdminMBean().getIndexDir();
            IndexReader reader = IndexReader.open(new SimpleFSDirectory(new File(dirName)),true);
            ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);
            archiveList.add(zoiereader);
            zoiereader.incRef();
            list.add(zoiereader);
          } else
          {
            List<ZoieIndexReader<R>> oldlist = oldZoie.getIndexReaders();// already incRef.
            list.addAll(oldlist);
          }
        }
        _oldZoies.clear();
      }
      // add the index readers for the current realtime index
      List<ZoieIndexReader<R>> readers = _currentZoie.getIndexReaders(); // already incRef
      list.addAll(readers);
    } catch(CorruptIndexException e)
    {
      log.info(e);
    } finally
    {
      _shutdownLock.readLock().unlock();
    }
    return list;
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
  public void consume(Collection<DataEvent<V>> data)
      throws ZoieException
  {
    try
    {
      _shutdownLock.readLock().lock();
      if (_isShutdown)
      {
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
        _oldZoies.add(_currentZoie);
        _dirMgr = _dirMgrFactory.getDirectoryManager();
        _dirMgrFactory.clearRecentlyChanged();
        _currentZoie = createZoie(_dirMgr);
        _currentZoie.start();
        _currentZoie.consume(data);
      }
    } finally
    {
      _shutdownLock.readLock().unlock();
    }
  }
  
  public void shutdown()
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
      for(ZoieSystem<R, V> oldZoie : _oldZoies)
      {
        oldZoie.shutdown();
      }
      if (_currentZoie != null)
        _currentZoie.shutdown();
      for(ZoieIndexReader<R> r : archiveList)
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
      log.info("shut down");
    } finally
    {
      _shutdownLock.writeLock().unlock();
    }
  }

  /* (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#getVersion()
   */
  public long getVersion()
  {
    _currentVersion = Math.max(_currentVersion, _currentZoie.getCurrentVersion());
    return _currentVersion;
  }
  public static class NullDocIDMapperFactory implements DocIDMapperFactory
  {
    public static final NullDocIDMapperFactory INSTANCE = new NullDocIDMapperFactory();
    public DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader)
    {
      for(ZoieIndexReader<?>r : reader.getSequentialSubReaders())
      {
        r.setDocIDMapper(NullDocIDMapper.INSTANCE);
      }
      return NullDocIDMapper.INSTANCE;
    }  }
  public static class NullDocIDMapper implements DocIDMapper
  {
    public static final NullDocIDMapper INSTANCE = new NullDocIDMapper();
    public int getDocID(long uid)
    {
      return DocIDMapper.NOT_FOUND;
    }

    public Object getDocIDArray(long[] uids)
    {
      throw new UnsupportedOperationException();
    }

    public Object getDocIDArray(int[] uids)
    {
      throw new UnsupportedOperationException();
    }

    public int getReaderIndex(long uid)
    {
      throw new UnsupportedOperationException();
    }

    public int[] getStarts()
    {
      throw new UnsupportedOperationException();
    }

    public ZoieIndexReader[] getSubReaders()
    {
      throw new UnsupportedOperationException();
    }

    public int quickGetDocID(long uid)
    {
      throw new UnsupportedOperationException();
    }  
  }
}
