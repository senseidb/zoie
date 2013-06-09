package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.hourglass.mbean.HourglassAdmin;
import proj.zoie.hourglass.mbean.HourglassAdminMBean;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

public class Hourglass<R extends IndexReader, D> implements Zoie<R, D> {
  public static final Logger log = Logger.getLogger(Hourglass.class);
  private final HourglassDirectoryManagerFactory _dirMgrFactory;
  private final ZoieIndexableInterpreter<D> _interpreter;
  private final IndexReaderDecorator<R> _decorator;
  private final ZoieConfig _zConfig;
  private volatile ZoieSystem<R, D> _currentZoie;
  private volatile boolean _isShutdown = false;
  final ReentrantReadWriteLock _shutdownLock = new ReentrantReadWriteLock();
  private final ReentrantLock _consumeLock = new ReentrantLock();
  private final HourglassReaderManager<R, D> _readerMgr;
  private volatile String _currentVersion = null;
  private long _freshness = 1000;
  final HourGlassScheduler _scheduler;
  public volatile long SLA = 4; // getIndexReaders should return in 4ms or a warning is logged
  private List<HourglassListener<R, D>> _hourglassListeners;

  public Hourglass(HourglassDirectoryManagerFactory dirMgrFactory,
      ZoieIndexableInterpreter<D> interpreter, IndexReaderDecorator<R> readerDecorator,
      ZoieConfig zoieConfig, List<HourglassListener<R, D>> hourglassListeners) {
    _zConfig = zoieConfig;
    _dirMgrFactory = dirMgrFactory;
    if (hourglassListeners == null) {
      hourglassListeners = Collections.emptyList();
    } else {
      hourglassListeners = new CopyOnWriteArrayList<HourglassListener<R, D>>(hourglassListeners);
    }
    _hourglassListeners = hourglassListeners;
    _scheduler = _dirMgrFactory.getScheduler();
    _dirMgrFactory.clearRecentlyChanged();
    _interpreter = interpreter;
    _decorator = readerDecorator;
    List<ZoieMultiReader<R>> archives;
    List<ZoieSystem<R, D>> archiveZoies;
    if (_dirMgrFactory.getScheduler().isAppendOnly()) {
      archives = loadArchives();
      archiveZoies = Collections.emptyList();
    } else {
      archives = Collections.emptyList();
      archiveZoies = loadArchiveZoies();
    }
    _readerMgr = new HourglassReaderManager<R, D>(this, _dirMgrFactory, _decorator, archives,
        archiveZoies, hourglassListeners);
    _currentVersion = _dirMgrFactory.getArchivedVersion();
    _currentZoie = _readerMgr.retireAndNew(null);
    _currentZoie.start();
    _freshness = zoieConfig.getFreshness();
    log.info("start Hourglass at version: " + _currentVersion);
  }

  public Hourglass(HourglassDirectoryManagerFactory dirMgrFactory,
      ZoieIndexableInterpreter<D> interpreter, IndexReaderDecorator<R> readerDecorator,
      ZoieConfig zoieConfig) {
    this(dirMgrFactory, interpreter, readerDecorator, zoieConfig, Collections.<HourglassListener<R, D>> emptyList());
  }

  @SuppressWarnings("unchecked")
  public Hourglass(HourglassDirectoryManagerFactory dirMgrFactory,
      ZoieIndexableInterpreter<D> interpreter, IndexReaderDecorator<R> readerDecorator,
      ZoieConfig zoieConfig, HourglassListener<R, D> hourglassListener) {
    this(dirMgrFactory, interpreter, readerDecorator, zoieConfig, Arrays.asList(hourglassListener));
  }

  protected List<ZoieSystem<R, D>> loadArchiveZoies() {
    List<ZoieSystem<R, D>> archives = new ArrayList<ZoieSystem<R, D>>();
    long t0 = System.currentTimeMillis();
    List<File> dirs = _dirMgrFactory.getAllArchivedDirs();
    for (File dir : dirs) {
      try {
        DirectoryManager dirMgr = new DefaultDirectoryManager(dir, _dirMgrFactory.getMode());
        ZoieSystem<R, D> zoie = new ZoieSystem<R, D>(dirMgr, _interpreter, _decorator, _zConfig);
        zoie.start();
        archives.add(zoie);
      } catch (Exception e) {
        log.error("Load index: " + dir + " failed.", e);
      }
    }
    log.info("load " + dirs.size() + " archived indices of " + getSizeBytes() + " bytes in "
        + (System.currentTimeMillis() - t0) + "ms");
    return archives;
  }

  protected List<ZoieMultiReader<R>> loadArchives() {
    List<ZoieMultiReader<R>> archives = new ArrayList<ZoieMultiReader<R>>();
    long t0 = System.currentTimeMillis();
    List<Directory> dirs = _dirMgrFactory.getAllArchivedDirectories();
    for (Directory dir : dirs) {
      DirectoryReader reader;
      try {
        reader = DirectoryReader.open(dir);
        ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, _decorator);

        // Initialize docIdMapper
        DocIDMapper mapper = _zConfig.getDocidMapperFactory().getDocIDMapper(zoiereader);
        zoiereader.setDocIDMapper(mapper);

        archives.add(zoiereader);
      } catch (CorruptIndexException e) {
        log.error("corruptedIndex", e);
      } catch (IOException e) {
        log.error("IOException", e);
      }
    }
    log.info("load " + dirs.size() + " archived indices of " + getSizeBytes() + " bytes in "
        + (System.currentTimeMillis() - t0) + "ms");
    return archives;
  }

  ZoieSystem<R, D> createZoie(DirectoryManager dirmgr) {
    return new ZoieSystem<R, D>(dirmgr, _interpreter, _decorator, _zConfig);
  }

  public ZoieConfig getzConfig() {
    return _zConfig;
  }

  public ZoieSystem<R, D> getCurrentZoie() {
    return _currentZoie;
  }

  public HourglassDirectoryManagerFactory getDirMgrFactory() {
    return _dirMgrFactory;
  }

  /*
   * (non-Javadoc)
   * @see proj.zoie.api.IndexReaderFactory#getAnalyzer()
   */
  @Override
  public Analyzer getAnalyzer() {
    return _zConfig.getAnalyzer();
  }

  /**
   * return a list of ZoieMultiReaders. These readers are reference counted and this method
   * should be used in pair with returnIndexReaders(List<ZoieMultiReader<R>> readers) {@link #returnIndexReaders(List)}.
   * It is typical that we create a MultiReader from these readers. When creating MultiReader, it should be created with
   * the closeSubReaders parameter set to false in order to do reference counting correctly.
   * <br> If this indexing system is already shut down, then we return an empty list.
   * @see proj.zoie.hourglass.impl.Hourglass#returnIndexReaders(List)
   * @see proj.zoie.api.IndexReaderFactory#getIndexReaders()
   */
  @Override
  public List<ZoieMultiReader<R>> getIndexReaders() throws IOException {
    long t0 = System.currentTimeMillis();
    try {
      _shutdownLock.readLock().lock();
      if (_isShutdown) {
        log.warn("System already shut down. No search request allowed.");
        List<ZoieMultiReader<R>> list = new ArrayList<ZoieMultiReader<R>>();
        return list;// if already shutdown, return an empty list
      }
      try {
        cacheLock.lock();
        if (System.currentTimeMillis() - lastupdate > _freshness) {
          updateCachedReaders();
        }
        List<ZoieMultiReader<R>> rlist = list;
        for (ZoieMultiReader<R> r : rlist) {
          r.incZoieRef();
        }
        t0 = System.currentTimeMillis() - t0;
        if (t0 > SLA) {
          log.warn("getIndexReaders returned in " + t0 + "ms more than " + SLA + "ms");
        }
        return rlist;
      } finally {
        cacheLock.unlock();
      }
    } finally {
      _shutdownLock.readLock().unlock();
    }
  }

  /**
   * not thread safe. should be properly lock. Right now we have two places to use it
   * and locked by the shutdown lock. If it gets more complicated, we should use separate
   * lock.
   * @throws IOException
   */
  private void updateCachedReaders() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("updating reader cache");
    }
    List<ZoieMultiReader<R>> olist = list;
    returnIndexReaders(olist);
    if (log.isDebugEnabled()) {
      log.debug("getting new reader from reader cache");
    }
    list = _readerMgr.getIndexReaders();
    if (log.isDebugEnabled()) {
      log.debug("reader updated with size: " + list.size());
    }
    lastupdate = System.currentTimeMillis();
  }

  /**
   * not thread safe. should be properly lock. Right now we have two places to use it
   * and locked by the shutdown lock. If it gets more complicated, we should use separate
   * lock.
   */
  private void clearCachedReaders() {
    List<ZoieMultiReader<R>> olist = list;
    returnIndexReaders(olist);
    list = null;
    lastupdate = 0;
  }

  private volatile long lastupdate = 0;
  private volatile List<ZoieMultiReader<R>> list = new ArrayList<ZoieMultiReader<R>>();
  private final ReentrantLock cacheLock = new ReentrantLock();

  /*
   * (non-Javadoc)
   * @see proj.zoie.api.IndexReaderFactory#returnIndexReaders(java.util.List)
   */
  @Override
  public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
    long t0 = System.currentTimeMillis();
    _currentZoie.returnIndexReaders(readers);
    t0 = System.currentTimeMillis() - t0;
    if (t0 > SLA) {
      log.warn("returnIndexReaders returned in " + t0 + "ms more than " + SLA + "ms");
    }
  }

  private void clearFromArchives(Collection<DataEvent<D>> data) throws ZoieException {
    if (_dirMgrFactory.getScheduler().isAppendOnly()) return;

    if (data != null && data.size() > 0) {
      List<DataEvent<D>> deletes = new ArrayList<DataEvent<D>>(data.size());
      for (DataEvent<D> event : data) {
        if (event instanceof MarkerDataEvent) continue;
        deletes.add(new DataEvent<D>(event.getData(), event.getVersion(), true));
      }
      for (ZoieSystem<R, D> zoie : _readerMgr.getArchiveZoies()) {
        zoie.consume(deletes);
      }
    }
  }

  /*
   * (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#consume(java.util.Collection)
   */
  @Override
  public void consume(Collection<DataEvent<D>> data) throws ZoieException {
    try {
      _consumeLock.lock(); // one at a time so we don't mess up during forward rolling.
                           // also we cannot do two consumptions at the same time anyway.
      try {
        _shutdownLock.readLock().lock();
        if (data == null || data.size() == 0) return;
        if (_isShutdown) {
          log.warn("System already shut down. Rejects indexing request.");
          return; // if the system is already shut down, we don't do anything.
        }
        // need to check time boundary. When we hit boundary, we need to trigger DM to
        // use new dir for zoie and the old one will be archive.
        if (!_dirMgrFactory.updateDirectoryManager()) {
          clearFromArchives(data);
          _currentZoie.consume(data);
        } else {
          // new time period
          _currentZoie = _readerMgr.retireAndNew(_currentZoie);
          _currentZoie.start();
          clearFromArchives(data);
          _currentZoie.consume(data);
        }
      } finally {
        _shutdownLock.readLock().unlock();
      }
    } finally {
      _consumeLock.unlock();
    }
  }

  @Override
  public void shutdown() {
    try {
      _shutdownLock.writeLock().lock();
      if (_isShutdown) {
        log.info("system already shut down");
        return;
      }
      _isShutdown = true;
    } finally {
      _shutdownLock.writeLock().unlock();
    }
    clearCachedReaders();
    _readerMgr.shutdown();

    log.info("shut down complete.");
  }

  /*
   * (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#getVersion()
   */
  @Override
  public String getVersion() {
    // _currentVersion = Math.max(_currentVersion, _currentZoie.getCurrentVersion());
    if (_currentZoie.getCurrentVersion() != null) {
      if (_currentVersion == null) {
        return _currentVersion = _currentZoie.getCurrentVersion();
      } else {
        _currentVersion = _zConfig.getVersionComparator().compare(_currentZoie.getCurrentVersion(),
          _currentVersion) < 0 ? _currentVersion : _currentZoie.getCurrentVersion();
      }
    }

    return _currentVersion;
  }

  /*
   * (non-Javadoc)
   * @see proj.zoie.api.DataConsumer#getVersionComparator()
   */
  @Override
  public Comparator<String> getVersionComparator() {
    return _zConfig.getVersionComparator();
  }

  public long getSizeBytes() {
    return _dirMgrFactory.getDiskIndexSizeBytes();
  }

  @Override
  public void syncWithVersion(long timeInMillis, String version) throws ZoieException {
    if (_currentZoie != null) {
      _currentZoie.syncWithVersion(timeInMillis, version);
    }
  }

  @Override
  public StandardMBean getStandardMBean(String name) {
    if (name.equals(HOURGLASSADMIN)) {
      try {
        return new StandardMBean(getAdminMBean(), HourglassAdminMBean.class);
      } catch (NotCompliantMBeanException e) {
        log.info(e);
        return null;
      }
    }
    return null;
  }

  @Override
  public HourglassAdminMBean getAdminMBean() {
    return new HourglassAdmin(this);
  }

  public static String HOURGLASSADMIN = "hourglass-admin";

  @Override
  public String[] getStandardMBeanNames() {
    return new String[] { HOURGLASSADMIN };
  }

  @Override
  public void start() {
    log.info("starting Hourglass... already done due by auto-start");
  }

  @Override
  public void flushEvents(long timeout) throws ZoieException {
    _currentZoie.flushEvents(timeout);
  }

  @Override
  public String getCurrentReaderVersion() {
    return _currentZoie == null ? null : _currentZoie.getCurrentReaderVersion();
  }

  public void addHourglassListener(HourglassListener<R, D> hourglassListener) {
    if (hourglassListener != null) {
      _hourglassListeners.add(hourglassListener);
    }
  }
}
