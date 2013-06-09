package proj.zoie.impl.indexing;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;

public class DefaultReaderCache<R extends IndexReader> extends AbstractReaderCache<R> {
  private static final Logger log = Logger.getLogger(DefaultReaderCache.class);
  private final Thread _maintenance;
  private volatile boolean alreadyShutdown = false;
  private volatile List<ZoieMultiReader<R>> cachedreaders = new ArrayList<ZoieMultiReader<R>>(0);
  private volatile long cachedreaderTimestamp = 0;
  private final ReentrantReadWriteLock cachedreadersLock = new ReentrantReadWriteLock();
  private volatile ConcurrentLinkedQueue<List<ZoieMultiReader<R>>> returningIndexReaderQueue = new ConcurrentLinkedQueue<List<ZoieMultiReader<R>>>();
  private final ReentrantReadWriteLock returningIndexReaderQueueLock = new ReentrantReadWriteLock();
  private final Object cachemonitor = new Object();
  private long _freshness = 10000L;
  private final WeakReference<IndexReaderFactory<R>> _readerfactory;

  public DefaultReaderCache(IndexReaderFactory<R> readerfactory) {
    _readerfactory = new WeakReference<IndexReaderFactory<R>>(readerfactory);
    _maintenance = newMaintenanceThread();
    _maintenance.setDaemon(true);
  }

  @Override
  public List<ZoieMultiReader<R>> getIndexReaders() {
    cachedreadersLock.readLock().lock();
    List<ZoieMultiReader<R>> readers = cachedreaders;
    for (ZoieMultiReader<R> r : readers) {
      r.incZoieRef();
    }
    cachedreadersLock.readLock().unlock();
    return readers;
  }

  @Override
  public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
    if (readers == null || readers.size() == 0) return;
    returningIndexReaderQueueLock.readLock().lock();
    try {
      returningIndexReaderQueue.add(readers);
    } finally {
      returningIndexReaderQueueLock.readLock().unlock();
    }
  }

  @Override
  public void refreshCache(long timeout) throws ZoieException {
    long begintime = System.currentTimeMillis();
    while (cachedreaderTimestamp <= begintime) {
      synchronized (cachemonitor) {
        cachemonitor.notifyAll();
        long elapsed = System.currentTimeMillis() - begintime;
        if (elapsed > timeout) {
          log.debug("refreshCached reader timeout in " + elapsed + "ms");
          throw new ZoieException("refreshCached reader timeout in " + elapsed + "ms");
        }
        long timetowait = Math.min(timeout - elapsed, 200);
        try {
          cachemonitor.wait(timetowait);
        } catch (InterruptedException e) {
          log.warn("refreshCache", e);
        }
      }
    }
  }

  @Override
  public void shutdown() {
    _freshness = 30000L;
    alreadyShutdown = true;
  }

  @Override
  public void start() {
    _maintenance.start();
  }

  @Override
  public long getFreshness() {
    return _freshness;
  }

  @Override
  public void setFreshness(long freshness) {
    _freshness = freshness;
  }

  private Thread newMaintenanceThread() {
    return new MaintenanceThread();
  }

  private class MaintenanceThread extends Thread {
    public MaintenanceThread() {
      super("DefaultReaderCache-zoie-indexReader-maintenance");
    }

    @Override
    public void run() {
      while (true) {
        try {
          synchronized (cachemonitor) {
            cachemonitor.wait(_freshness);
          }
        } catch (InterruptedException e) {
          Thread.interrupted(); // clear interrupted state
        }
        List<ZoieMultiReader<R>> newreaders = null;
        if (alreadyShutdown) {
          newreaders = new ArrayList<ZoieMultiReader<R>>();
          // clean up and quit
        } else {
          try {
            IndexReaderFactory<R> readerfactory = _readerfactory.get();
            if (readerfactory != null) {
              newreaders = readerfactory.getIndexReaders();
            } else {
              newreaders = new ArrayList<ZoieMultiReader<R>>();
            }
          } catch (IOException e) {
            log.info("DefaultReaderCache-zoie-indexReader-maintenance", e);
            newreaders = new ArrayList<ZoieMultiReader<R>>();
          }
        }
        List<ZoieMultiReader<R>> oldreaders = cachedreaders;
        cachedreadersLock.writeLock().lock();
        cachedreaders = newreaders;
        cachedreadersLock.writeLock().unlock();
        cachedreaderTimestamp = System.currentTimeMillis();
        synchronized (cachemonitor) {
          cachemonitor.notifyAll();
        }
        // return the old cached reader
        if (!oldreaders.isEmpty()) returnIndexReaders(oldreaders);
        // process the returing index reader queue
        returningIndexReaderQueueLock.writeLock().lock();
        ConcurrentLinkedQueue<List<ZoieMultiReader<R>>> oldreturningIndexReaderQueue = returningIndexReaderQueue;
        returningIndexReaderQueue = new ConcurrentLinkedQueue<List<ZoieMultiReader<R>>>();
        returningIndexReaderQueueLock.writeLock().unlock();
        for (List<ZoieMultiReader<R>> readers : oldreturningIndexReaderQueue) {
          for (ZoieMultiReader<R> r : readers) {
            r.decZoieRef();
          }
        }
        if (_readerfactory.get() == null && cachedreaders.size() == 0) {
          log.info("ZoieSystem has been GCed. Exiting DefaultReaderCache Maintenance Thread "
              + this);
          break; // if the system is GCed, we quit.
        }
      }
    }
  }

  public static ReaderCacheFactory FACTORY = new ReaderCacheFactory() {

    @Override
    public <R extends IndexReader> AbstractReaderCache<R> newInstance(
        IndexReaderFactory<R> readerfactory) {
      return new DefaultReaderCache<R>(readerfactory);
    }
  };
}
