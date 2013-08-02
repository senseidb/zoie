package proj.zoie.pair.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.IndexCopier;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.mbean.ZoieAdminMBean;

public class Pair<R extends IndexReader, D> implements Zoie<R, D> {
  public static final Logger log = Logger.getLogger(Pair.class);

  private static final String COMMIT_FILE = "committed";

  private volatile boolean _running = false;

  private volatile Zoie<R, D> _zoieOne;
  private final Zoie<R, D> _zoieTwo; // The realtime zoie.

  private final File _zoieOneRoot;
  private final IndexCopier _indexCopier;
  private final ZoieIndexableInterpreter<D> _interpreter;
  private final IndexReaderDecorator<R> _decorator;
  private final ZoieConfig _zoieConfig;
  private final Map<IndexReader, ZoieRef> _activeReaders;

  public Pair(File zoieOneRoot, DIRECTORY_MODE dirMode, IndexCopier indexCopier,
      ZoieIndexableInterpreter<D> interpreter, IndexReaderDecorator<R> decorator,
      ZoieConfig zoieConfig, Zoie<R, D> zoieTwo) {
    _zoieOneRoot = zoieOneRoot;
    _indexCopier = indexCopier;
    _interpreter = interpreter;
    _decorator = decorator;
    _zoieConfig = zoieConfig;
    _zoieTwo = zoieTwo;
    _activeReaders = new HashMap<IndexReader, ZoieRef>();

    // Initialize zoieOne:
    if (_zoieOneRoot == null) {
      throw new IllegalArgumentException("zoieOneRoot cannot be null.");
    }
    if (_zoieOneRoot.exists()) {
      File[] files = _zoieOneRoot.listFiles();
      Arrays.sort(files);
      for (int i = files.length - 1; i >= 0; --i) {
        if (new File(files[i], COMMIT_FILE).exists()) {
          log.info("Found latest zoieOne index: " + files[i].getAbsolutePath());
          DirectoryManager dirMgr = new DefaultDirectoryManager(files[i], dirMode);
          _zoieOne = new ZoieSystem<R, D>(dirMgr, _interpreter, _decorator, _zoieConfig);
          break;
        }
      }
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public synchronized boolean loadIndex(String src) {
    File dest = new File(_zoieOneRoot, String.valueOf(System.currentTimeMillis()));
    while (!dest.mkdirs()) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // ignore
      }
      dest = new File(_zoieOneRoot, String.valueOf(System.currentTimeMillis()));
    }

    log.info("Copying " + src + " to " + dest.getAbsolutePath());

    if (!_indexCopier.copy(src, dest.getAbsolutePath())) return false;

    File directoryFile = new File(dest, DirectoryManager.INDEX_DIRECTORY);
    if (!directoryFile.exists()) {
      log.warn("index directory file not exists, creating a empty one.");
      IndexSignature sig = new IndexSignature(null);
      try {
        DefaultDirectoryManager.saveSignature(sig, directoryFile);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
        return false;
      }
    }

    try {
      // Touch the commit file:
      OutputStream out = new FileOutputStream(new File(dest, COMMIT_FILE));
      out.close();
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return false;
    }

    // offline line index is not moving, so MMAP should be the most efficient.
    DirectoryManager dirMgr = new DefaultDirectoryManager(dest, DIRECTORY_MODE.MMAP);
    Zoie<R, D> zoie = new ZoieSystem(dirMgr, _interpreter, _decorator, _zoieConfig);
    if (_running) zoie.start();

    if (_zoieOne != null) {
      final Zoie toBeShutdown = _zoieOne;
      _zoieOne = zoie;

      Timer cleanupTimer = new Timer();

      cleanupTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          synchronized (_activeReaders) {
            for (ZoieRef z : _activeReaders.values()) {
              if (z.zoie == toBeShutdown) {
                log.info("Waiting for active readers...");
                return;
              }
            }
          }
          log.info("Shuting down old zoie...");
          toBeShutdown.shutdown();
          cancel();
        }
      }, 8000, 8000);
    }

    _zoieOne = zoie;

    return true;
  }

  @Override
  public void start() {
    _running = true;
    Zoie<R, D> zoieOne = _zoieOne;
    if (zoieOne != null) {
      zoieOne.start();
    }

    if (_zoieTwo != null) {
      _zoieTwo.start();
    }
  }

  @Override
  public void shutdown() {
    _running = false;
    Zoie<R, D> zoieOne = _zoieOne;
    if (zoieOne != null) {
      zoieOne.shutdown();
    }

    if (_zoieTwo != null) {
      _zoieTwo.shutdown();
    }
  }

  @Override
  public StandardMBean getStandardMBean(String name) {
    if (name.equals(PAIRADMIN)) {
      try {
        return new StandardMBean(getAdminMBean(), PairAdminMBean.class);
      } catch (NotCompliantMBeanException e) {
        log.info(e);
        return null;
      }
    }
    return null;
  }

  @Override
  public PairAdminMBean getAdminMBean() {
    return new MyPairAdmin();
  }

  public static String PAIRADMIN = "pair-admin";

  @Override
  public String[] getStandardMBeanNames() {
    return new String[] { PAIRADMIN };
  }

  public interface PairAdminMBean extends ZoieAdminMBean {
    boolean loadIndex(String src);
  }

  private class MyPairAdmin implements PairAdminMBean {

    @Override
    public boolean loadIndex(String src) {
      return Pair.this.loadIndex(src);
    }

    @Override
    public String getIndexDir() {
      return Pair.this._zoieOneRoot.getAbsolutePath();
    }

    @Override
    public boolean isRealtime() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().isRealtime();

      return false;
    }

    @Override
    public long getBatchDelay() {
      return Pair.this._zoieConfig.getBatchDelay();
    }

    @Override
    public void setBatchDelay(long delay) {
      Pair.this._zoieConfig.setBatchDelay(delay);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setBatchDelay(delay);
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setBatchDelay(delay);
    }

    @Override
    public int getBatchSize() {
      return Pair.this._zoieConfig.getBatchSize();
    }

    @Override
    public void setBatchSize(int batchSize) {
      Pair.this._zoieConfig.setBatchSize(batchSize);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setBatchSize(batchSize);
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setBatchSize(batchSize);
    }

    @Override
    public Date getLastDiskIndexModifiedTime() {
      Date d1 = null, d2 = null;
      if (Pair.this._zoieTwo != null) d2 = Pair.this._zoieTwo.getAdminMBean()
          .getLastDiskIndexModifiedTime();

      d1 = Pair.this._zoieOne.getAdminMBean().getLastDiskIndexModifiedTime();

      if (d2 == null) return d1;
      else if (d1 == null) return d2;
      else if (d1.compareTo(d2) > 0) return d1;
      else return d2;
    }

    @Override
    public int getMaxBatchSize() {
      return Pair.this._zoieConfig.getMaxBatchSize();
    }

    @Override
    public void setMaxBatchSize(int maxBatchSize) {
      Pair.this._zoieConfig.setMaxBatchSize(maxBatchSize);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setMaxBatchSize(
        maxBatchSize);
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setMaxBatchSize(
        maxBatchSize);
    }

    @Override
    public void setMergeFactor(int mergeFactor) {
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean()
          .setMergeFactor(mergeFactor);
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean()
          .setMergeFactor(mergeFactor);
    }

    @Override
    public int getMergeFactor() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getMergeFactor();

      return Pair.this._zoieOne.getAdminMBean().getMergeFactor();
    }

    @Override
    public int getRamAIndexSize() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getRamAIndexSize();

      return 0;
    }

    @Override
    public String getRamAVersion() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getRamAVersion();

      return null;
    }

    @Override
    public int getRamBIndexSize() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getRamBIndexSize();

      return 0;
    }

    @Override
    public String getRamBVersion() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getRamBVersion();

      return null;
    }

    @Override
    public String getDiskIndexerStatus() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getDiskIndexerStatus();

      return Pair.this._zoieOne.getAdminMBean().getDiskIndexerStatus();
    }

    @Override
    public String getCurrentDiskVersion() throws IOException {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getCurrentDiskVersion();

      return Pair.this._zoieOne.getAdminMBean().getCurrentDiskVersion();
    }

    @Override
    public void refreshDiskReader() throws IOException {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().refreshDiskReader();
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().refreshDiskReader();
    }

    @Override
    public void flushToDiskIndex() throws ZoieException {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().flushToDiskIndex();
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().flushToDiskIndex();
    }

    @Override
    public void flushToMemoryIndex() throws ZoieException {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().flushToMemoryIndex();
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().flushToMemoryIndex();
    }

    @Override
    public int getMaxMergeDocs() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getMaxMergeDocs();

      return Pair.this._zoieOne.getAdminMBean().getMaxMergeDocs();
    }

    @Override
    public void setMaxMergeDocs(int maxMergeDocs) {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setMaxMergeDocs(
        maxMergeDocs);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setMaxMergeDocs(
        maxMergeDocs);
    }

    @Override
    public void setNumLargeSegments(int numLargeSegments) {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setNumLargeSegments(
        numLargeSegments);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setNumLargeSegments(
        numLargeSegments);
    }

    @Override
    public int getNumLargeSegments() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getNumLargeSegments();

      return Pair.this._zoieOne.getAdminMBean().getNumLargeSegments();
    }

    @Override
    public void setMaxSmallSegments(int maxSmallSegments) {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setMaxSmallSegments(
        maxSmallSegments);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setMaxSmallSegments(
        maxSmallSegments);
    }

    @Override
    public int getMaxSmallSegments() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getMaxSmallSegments();

      return Pair.this._zoieOne.getAdminMBean().getMaxSmallSegments();
    }

    @Override
    public long getDiskIndexSizeBytes() {
      long size = 0;
      if (Pair.this._zoieTwo != null) size = Pair.this._zoieTwo.getAdminMBean()
          .getDiskIndexSizeBytes();
      if (Pair.this._zoieOne != null) size += Pair.this._zoieTwo.getAdminMBean()
          .getDiskIndexSizeBytes();

      return size;
    }

    @Override
    public long getDiskFreeSpaceBytes() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getDiskFreeSpaceBytes();

      return Pair.this._zoieOne.getAdminMBean().getDiskFreeSpaceBytes();
    }

    @Override
    public boolean isUseCompoundFile() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().isUseCompoundFile();

      return Pair.this._zoieOne.getAdminMBean().isUseCompoundFile();
    }

    @Override
    public int getDiskIndexSegmentCount() throws IOException {
      int count = 0;
      if (Pair.this._zoieTwo != null) count = Pair.this._zoieTwo.getAdminMBean()
          .getDiskIndexSegmentCount();
      if (Pair.this._zoieOne != null) count += Pair.this._zoieTwo.getAdminMBean()
          .getDiskIndexSegmentCount();

      return count;
    }

    @Override
    public int getRAMASegmentCount() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getRAMASegmentCount();

      return 0;
    }

    @Override
    public int getRAMBSegmentCount() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getRAMBSegmentCount();

      return 0;
    }

    @Override
    public long getSLA() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean().getSLA();

      return Pair.this._zoieOne.getAdminMBean().getSLA();
    }

    @Override
    public void setSLA(long sla) {
      if (Pair.this._zoieTwo != null) Pair.this._zoieTwo.getAdminMBean().setSLA(sla);
      if (Pair.this._zoieOne != null) Pair.this._zoieOne.getAdminMBean().setSLA(sla);
    }

    @Override
    public long getHealth() {
      return ZoieHealth.getHealth();
    }

    @Override
    public void resetHealth() {
      ZoieHealth.setOK();
    }

    @Override
    public int getCurrentMemBatchSize() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getCurrentMemBatchSize();

      return 0;
    }

    @Override
    public int getCurrentDiskBatchSize() {
      if (Pair.this._zoieTwo != null) return Pair.this._zoieTwo.getAdminMBean()
          .getCurrentDiskBatchSize();

      return Pair.this._zoieOne.getAdminMBean().getCurrentDiskBatchSize();
    }
  }

  @Override
  public void syncWithVersion(long timeInMillis, String version) throws ZoieException {
    if (_zoieTwo != null) {
      _zoieTwo.syncWithVersion(timeInMillis, version);
    }
  }

  @Override
  public void flushEvents(long timeout) throws ZoieException {
    if (_zoieTwo != null) {
      _zoieTwo.flushEvents(timeout);
    }
  }

  @Override
  public void consume(Collection<DataEvent<D>> data) throws ZoieException {
    if (_zoieTwo != null) {
      _zoieTwo.consume(data);
    }
  }

  @Override
  public String getVersion() {
    String v1 = null, v2 = null;
    Zoie<R, D> zoieOne = _zoieOne;
    if (zoieOne != null) {
      v1 = zoieOne.getVersion();
    }

    if (_zoieTwo != null) {
      v2 = _zoieTwo.getVersion();
    }

    return _zoieConfig.getVersionComparator().compare(v2, v1) > 0 ? v2 : v1;
  }

  @Override
  public String getCurrentReaderVersion() {
    String v1 = null, v2 = null;
    Zoie<R, D> zoieOne = _zoieOne;
    if (zoieOne != null) {
      v1 = zoieOne.getCurrentReaderVersion();
    }

    if (_zoieTwo != null) {
      v2 = _zoieTwo.getCurrentReaderVersion();
    }

    return _zoieConfig.getVersionComparator().compare(v2, v1) > 0 ? v2 : v1;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return _zoieConfig.getVersionComparator();
  }

  @Override
  public List<ZoieMultiReader<R>> getIndexReaders() throws IOException {
    List<ZoieMultiReader<R>> readers = new ArrayList<ZoieMultiReader<R>>();

    if (_zoieTwo != null) {
      readers.addAll(_zoieTwo.getIndexReaders());
    }

    Zoie<R, D> zoieOne = _zoieOne;
    if (zoieOne != null) {
      List<ZoieMultiReader<R>> zoieOneReaders = zoieOne.getIndexReaders();
      for (ZoieMultiReader<R> r : zoieOneReaders) {
        synchronized (_activeReaders) {
          ZoieRef zoieRef = _activeReaders.get(r);
          if (zoieRef != null) ++zoieRef.refCount;
          else {
            zoieRef = new ZoieRef(zoieOne);
            _activeReaders.put(r, zoieRef);
          }
        }
      }
      readers.addAll(zoieOneReaders);
    }

    return readers;
  }

  @Override
  public Analyzer getAnalyzer() {
    return _zoieConfig.getAnalyzer();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
    if (readers != null) {
      Map<Zoie<R, D>, List<ZoieMultiReader<R>>> destMap = new HashMap<Zoie<R, D>, List<ZoieMultiReader<R>>>();
      for (ZoieMultiReader<R> r : readers) {
        Zoie zoie = _zoieTwo;

        synchronized (_activeReaders) {
          ZoieRef zoieRef = _activeReaders.get(r);

          if (zoieRef != null) {
            zoie = zoieRef.zoie;
            --zoieRef.refCount;
            if (zoieRef.refCount <= 0) _activeReaders.remove(r);
          }
        }

        List<ZoieMultiReader<R>> readerList = destMap.get(zoie);
        if (readerList == null) {
          readerList = new ArrayList<ZoieMultiReader<R>>();
          destMap.put(zoie, readerList);
        }
        readerList.add(r);
      }

      for (Map.Entry<Zoie<R, D>, List<ZoieMultiReader<R>>> entry : destMap.entrySet()) {
        entry.getKey().returnIndexReaders(entry.getValue());
      }
    }
  }

  private static class ZoieRef {
    public Zoie<?, ?> zoie;
    public int refCount;

    public ZoieRef(Zoie<?, ?> zoie) {
      this.zoie = zoie;
    }
  }
}
