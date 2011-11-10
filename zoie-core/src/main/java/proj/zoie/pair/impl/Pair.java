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
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.api.IndexCopier;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.mbean.ZoieAdminMBean;

public class Pair<R extends IndexReader, D> implements Zoie<R, D>
{
  public static final Logger log = Logger.getLogger(Pair.class);

  private static final String COMMIT_FILE = "committed";

  private volatile boolean _running = false;

  private volatile Zoie<R, D> _zoieOne;
  private final    Zoie<R, D> _zoieTwo; // The realtime zoie.

  private final File                        _zoieOneRoot;
  private final IndexCopier                 _indexCopier;
  private final ZoieIndexableInterpreter<D> _interpreter;
  private final IndexReaderDecorator<R>     _decorator;
  private final ZoieConfig                  _zoieConfig;

  private Map<IndexReader, ZoieRef> _activeReaders;

  public Pair(File                          zoieOneRoot,
              IndexCopier                   indexCopier,
              ZoieIndexableInterpreter<D>   interpreter,
              IndexReaderDecorator<R>       decorator,
              ZoieConfig                    zoieConfig,
              Zoie<R, D>                    zoieTwo)
  {
    _zoieOneRoot = zoieOneRoot;
    _indexCopier = indexCopier;
    _interpreter = interpreter;
    _decorator   = decorator;
    _zoieConfig  = zoieConfig;
    _zoieTwo     = zoieTwo;

    _activeReaders = new HashMap<IndexReader, ZoieRef>();

    // Initialize zoieOne:
    if (_zoieOneRoot == null)
      throw new IllegalArgumentException("zoieOneRoot cannot be null.");
    if (_zoieOneRoot.exists())
    {
      File[] files = _zoieOneRoot.listFiles();
      Arrays.sort(files);
      for (int i = files.length - 1; i >= 0; --i)
      {
        if (new File(files[i], COMMIT_FILE).exists())
        {
          log.info("Found latest zoieOne index: " + files[i].getAbsolutePath());
          _zoieOne = new ZoieSystem(files[i], _interpreter, _decorator, _zoieConfig);
          break;
        }
      }
    }
  }

  public synchronized boolean loadIndex(String src)
  {
    File dest = new File(_zoieOneRoot, String.valueOf(System.currentTimeMillis()));
    while(!dest.mkdirs())
    {
      try
      {
        Thread.sleep(1);
      }
      catch(InterruptedException e)
      {
        // ignore
      }
      dest = new File(_zoieOneRoot, String.valueOf(System.currentTimeMillis()));
    }

    log.info("Copying " + src + " to " + dest.getAbsolutePath());

    if (!_indexCopier.copy(src, dest.getAbsolutePath()))
      return false;

    File directoryFile = new File(dest, DirectoryManager.INDEX_DIRECTORY);
    if (!directoryFile.exists())
    {
      log.warn("index directory file not exists, creating a empty one.");
      IndexSignature sig = new IndexSignature(null);
      try
      {
        DefaultDirectoryManager.saveSignature(sig, directoryFile);
      }
      catch (IOException e)
      {
        log.error(e.getMessage(), e);
        return false;
      }
    }

    try
    {
      // Touch the commit file:
      OutputStream out = new FileOutputStream(new File(dest, COMMIT_FILE));
      out.close();
    }
    catch(Exception e)
    {
      log.error(e.getMessage(), e);
      return false;
    }

    Zoie zoie = new ZoieSystem(dest, _interpreter, _decorator, _zoieConfig);
    if (_running)
      zoie.start();

    if (_zoieOne != null)
    {
      final Zoie toBeShutdown = _zoieOne;
      _zoieOne = zoie;

      Timer cleanupTimer = new Timer();

      cleanupTimer.schedule(new TimerTask()
      {
        @Override
        public void run()
        {
          synchronized(_activeReaders)
          {
            for (ZoieRef z : _activeReaders.values())
            {
              if (z.zoie == toBeShutdown)
              {
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

  public void start()
  {
    _running = true;
    Zoie zoieOne = _zoieOne;
    if (zoieOne != null)
      zoieOne.start();

    if (_zoieTwo != null)
      _zoieTwo.start();
  }

  public void shutdown()
  {
    _running = false;
    Zoie zoieOne = _zoieOne;
    if (zoieOne != null)
      zoieOne.shutdown();

    if (_zoieTwo != null)
      _zoieTwo.shutdown();
  }

  public StandardMBean getStandardMBean(String name)
  {
    if (name.equals(PAIRADMIN))
    {
      try
      {
        return new StandardMBean(getAdminMBean(), PairAdminMBean.class);
      }
      catch (NotCompliantMBeanException e)
      {
        log.info(e);
        return null;
      }
    }
    return null;
  }

  public PairAdminMBean getAdminMBean()
  {
    return new MyPairAdmin();
  }

  public static String PAIRADMIN = "pair-admin";
  public String[] getStandardMBeanNames()
  {
    return new String[] { PAIRADMIN };
  }

  public interface PairAdminMBean extends ZoieAdminMBean
  {
    boolean loadIndex(String src);
  }

  private class MyPairAdmin implements PairAdminMBean
  {

    public boolean loadIndex(String src)
    {
      return Pair.this.loadIndex(src);
    }

    public String getIndexDir()
    {
      return Pair.this._zoieOneRoot.getAbsolutePath();
    }

    public boolean isRealtime()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().isRealtime();

      return false;
    }

    public long getBatchDelay()
    {
      return Pair.this._zoieConfig.getBatchDelay();
    }

    public void setBatchDelay(long delay)
    {
      Pair.this._zoieConfig.setBatchDelay(delay);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setBatchDelay(delay);
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setBatchDelay(delay);
    }

    public int getBatchSize()
    {
      return Pair.this._zoieConfig.getBatchSize();
    }

    public void setBatchSize(int batchSize)
    {
      Pair.this._zoieConfig.setBatchSize(batchSize);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setBatchSize(batchSize);
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setBatchSize(batchSize);
    }

    public Date getLastDiskIndexModifiedTime()
    {
      Date d1 = null, d2 = null;
      if (Pair.this._zoieTwo != null)
        d2 = Pair.this._zoieTwo.getAdminMBean().getLastDiskIndexModifiedTime();

      d1 = Pair.this._zoieOne.getAdminMBean().getLastDiskIndexModifiedTime();

      if (d2 == null)
        return d1;
      else if (d1 == null)
        return d2;
      else if (d1.compareTo(d2) > 0)
        return d1;
      else
        return d2;
    }

    public int getMaxBatchSize()
    {
      return Pair.this._zoieConfig.getMaxBatchSize();
    }

    public void setMaxBatchSize(int maxBatchSize)
    {
      Pair.this._zoieConfig.setMaxBatchSize(maxBatchSize);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setMaxBatchSize(maxBatchSize);
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setMaxBatchSize(maxBatchSize);
    }

    public void setMergeFactor(int mergeFactor)
    {
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setMergeFactor(mergeFactor);
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setMergeFactor(mergeFactor);
    }

    public int getMergeFactor()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getMergeFactor();

      return Pair.this._zoieOne.getAdminMBean().getMergeFactor();
    }

    public int getRamAIndexSize()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRamAIndexSize();

      return 0;
    }

    public String getRamAVersion()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRamAVersion();

      return null;
    }

    public int getRamBIndexSize()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRamBIndexSize();

      return 0;
    }

    public String getRamBVersion()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRamBVersion();

      return null;
    }

    public String getDiskIndexerStatus()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getDiskIndexerStatus();

      return Pair.this._zoieOne.getAdminMBean().getDiskIndexerStatus();
    }

    public String getCurrentDiskVersion() throws IOException
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getCurrentDiskVersion();

      return Pair.this._zoieOne.getAdminMBean().getCurrentDiskVersion();
    }

    public void refreshDiskReader() throws IOException
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().refreshDiskReader();
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().refreshDiskReader();
    }

    public void flushToDiskIndex() throws ZoieException
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().flushToDiskIndex();
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().flushToDiskIndex();
    }

    public void flushToMemoryIndex() throws ZoieException
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().flushToMemoryIndex();
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().flushToMemoryIndex();
    }

    public int getMaxMergeDocs()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getMaxMergeDocs();

      return Pair.this._zoieOne.getAdminMBean().getMaxMergeDocs();
    }

    public void setMaxMergeDocs(int maxMergeDocs)
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setMaxMergeDocs(maxMergeDocs);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setMaxMergeDocs(maxMergeDocs);
    }
    
    public void setNumLargeSegments(int numLargeSegments)
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setNumLargeSegments(numLargeSegments);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setNumLargeSegments(numLargeSegments);
    }

    public int getNumLargeSegments()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getNumLargeSegments();

      return Pair.this._zoieOne.getAdminMBean().getNumLargeSegments();
    }

    public void setMaxSmallSegments(int maxSmallSegments)
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setMaxSmallSegments(maxSmallSegments);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setMaxSmallSegments(maxSmallSegments);
    }

    public int getMaxSmallSegments()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getMaxSmallSegments();

      return Pair.this._zoieOne.getAdminMBean().getMaxSmallSegments();
    }
    
    public long getDiskIndexSizeBytes()
    {
      long size = 0;
      if (Pair.this._zoieTwo != null)
        size = Pair.this._zoieTwo.getAdminMBean().getDiskIndexSizeBytes();
      if (Pair.this._zoieOne != null)
        size += Pair.this._zoieTwo.getAdminMBean().getDiskIndexSizeBytes();

      return size;
    }

    public long getDiskFreeSpaceBytes()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getDiskFreeSpaceBytes();

      return Pair.this._zoieOne.getAdminMBean().getDiskFreeSpaceBytes();
    }

    public boolean isUseCompoundFile()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().isUseCompoundFile();

      return Pair.this._zoieOne.getAdminMBean().isUseCompoundFile();
    }

    public int getDiskIndexSegmentCount() throws IOException
    {
      int count = 0;
      if (Pair.this._zoieTwo != null)
        count = Pair.this._zoieTwo.getAdminMBean().getDiskIndexSegmentCount();
      if (Pair.this._zoieOne != null)
        count += Pair.this._zoieTwo.getAdminMBean().getDiskIndexSegmentCount();

      return count;
    }

    public int getRAMASegmentCount()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRAMASegmentCount();

      return 0;
    }

    public int getRAMBSegmentCount()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getRAMBSegmentCount();

      return 0;
    }

    public long getSLA()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getSLA();

      return Pair.this._zoieOne.getAdminMBean().getSLA();
    }
    
    public void setSLA(long sla)
    {
      if (Pair.this._zoieTwo != null)
        Pair.this._zoieTwo.getAdminMBean().setSLA(sla);
      if (Pair.this._zoieOne != null)
        Pair.this._zoieOne.getAdminMBean().setSLA(sla);
    }
    
    public long getHealth()
    {
      return ZoieHealth.getHealth();
    }
    
    public void resetHealth()
    {
      ZoieHealth.setOK();
    }
    
    public int getCurrentMemBatchSize()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getCurrentMemBatchSize();

      return 0;
    }

    public int getCurrentDiskBatchSize()
    {
      if (Pair.this._zoieTwo != null)
        return Pair.this._zoieTwo.getAdminMBean().getCurrentDiskBatchSize();

      return Pair.this._zoieOne.getAdminMBean().getCurrentDiskBatchSize();
    }
  }

  public void syncWithVersion(long timeInMillis, String version) throws ZoieException
  {
    //long t0 = System.currentTimeMillis();

    //Zoie zoieOne = _zoieOne;
    //if (zoieOne != null)  // TODO:last zoieOne version.
      //zoieOne.syncWithVersion(timeInMillis, version);

    //long left = timeInMillis - (System.currentTimeMillis()-t0);

    //if (left > 0)
      //_zoieTwo.syncWithVersion(timeInMillis, version);
    //else
      //throw new ZoieException("timed out when syncing.");

    if (_zoieTwo != null)
      _zoieTwo.syncWithVersion(timeInMillis, version);
  }

  public void flushEvents(long timeout) throws ZoieException
  {
    //long t0 = System.currentTimeMillis();

    //Zoie zoieOne = _zoieOne;
    //if (zoieOne != null)
      //zoieOne.flushEvents(timeout);

    //long left = timeout - (System.currentTimeMillis()-t0);

    //if (left > 0)
      //_zoieTwo.flushEvents(left);
    //else
      //throw new ZoieException("timed out when flushing.");

    if (_zoieTwo != null)
      _zoieTwo.flushEvents(timeout);
  }

  public void consume(Collection<DataEvent<D>> data) throws ZoieException
  {
    if (_zoieTwo != null)
      _zoieTwo.consume(data);
  }

  public String getVersion()
  {
    String v1 = null, v2 = null;
    Zoie zoieOne = _zoieOne;
    if (zoieOne != null)
      v1 = zoieOne.getVersion();

    if (_zoieTwo != null)
      v2 = _zoieTwo.getVersion();

    return _zoieConfig.getVersionComparator().compare(v2, v1) > 0 ? v2 : v1;
  }

  @Override
  public String getCurrentReaderVersion()
  {
    String v1 = null, v2 = null;
    Zoie zoieOne = _zoieOne;
    if (zoieOne != null)
      v1 = zoieOne.getCurrentReaderVersion();

    if (_zoieTwo != null)
      v2 = _zoieTwo.getCurrentReaderVersion();

    return _zoieConfig.getVersionComparator().compare(v2, v1) > 0 ? v2 : v1;
  }

	public Comparator<String> getVersionComparator()
  {
    return _zoieConfig.getVersionComparator();
  }

  public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
  {
    List<ZoieIndexReader<R>> readers = new ArrayList<ZoieIndexReader<R>>();

    if (_zoieTwo != null)
      readers.addAll(_zoieTwo.getIndexReaders());

    Zoie zoieOne = _zoieOne;
    if (zoieOne != null)
    {
      List<ZoieIndexReader<R>> r1 = zoieOne.getIndexReaders();
      for (ZoieIndexReader<R> r : r1)
      {
        synchronized(_activeReaders)
        {
          ZoieRef zoieRef = _activeReaders.get(r);
          if (zoieRef != null)
            ++zoieRef.refCount;
          else
          {
            zoieRef = new ZoieRef(zoieOne);
            _activeReaders.put(r, zoieRef);
          }
        }
      }
      readers.addAll(r1);
    }

    return readers;
  }

  public Analyzer getAnalyzer()
  {
    return _zoieConfig.getAnalyzer();
  }

  public void returnIndexReaders(List<ZoieIndexReader<R>> readers)
  {
    if (readers != null)
    {
      Map<Zoie<R, D>, List<ZoieIndexReader<R>>> destMap = new HashMap<Zoie<R, D>, List<ZoieIndexReader<R>>>();
      for (ZoieIndexReader<R> r : readers)
      {
        Zoie zoie = _zoieTwo;

        synchronized(_activeReaders)
        {
          ZoieRef zoieRef = _activeReaders.get(r);

          if (zoieRef != null)
          {
            zoie = zoieRef.zoie;
            --zoieRef.refCount;
            if (zoieRef.refCount <= 0)
              _activeReaders.remove(r);
          }
        }

        List<ZoieIndexReader<R>> readerList = destMap.get(zoie);
        if (readerList == null)
        {
          readerList = new ArrayList<ZoieIndexReader<R>>();
          destMap.put(zoie, readerList);
        }
        readerList.add(r);
      }

      for(Map.Entry<Zoie<R, D>, List<ZoieIndexReader<R>>> entry : destMap.entrySet())
      {
        entry.getKey().returnIndexReaders(entry.getValue());
      }
    }
  }

  private static class ZoieRef
  {
    public Zoie<?, ?> zoie;
    public int refCount;

    public ZoieRef(Zoie<?, ?> zoie)
    {
      this.zoie = zoie;
    }
  }
}
