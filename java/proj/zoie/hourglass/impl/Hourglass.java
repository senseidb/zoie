package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.ZoieSystem;

public class Hourglass<R extends IndexReader, V> implements IndexReaderFactory<ZoieIndexReader<R>>, DataConsumer<V>
{
  private final HourglassDirectoryManagerFactory _dirMgrFactory;
  private DirectoryManager _dirMgr;
  private ZoieIndexableInterpreter<V> _interpreter;
  private IndexReaderDecorator<R> _decorator;
  private Analyzer _analyzer;
  private Similarity _similarity;
  private int _batchSize;
  private long _batchDelay;
  private volatile ZoieSystem<R, V> _currentZoie;
  private volatile ZoieSystem<R, V> _oldZoie = null;
  private final List<ZoieIndexReader<R>> archiveList = new ArrayList<ZoieIndexReader<R>>();
  public Hourglass(HourglassDirectoryManagerFactory dirMgrFactory, ZoieIndexableInterpreter<V> interpreter,
      IndexReaderDecorator<R> readerDecorator, Analyzer analyzer, Similarity similarity,
      int batchSize,long batchDelay)
  {
    _dirMgrFactory = dirMgrFactory;
    _dirMgr = _dirMgrFactory.getDirectoryManager();
    _dirMgrFactory.clearRecentlyChanged();
    _interpreter = interpreter;
    _decorator = readerDecorator;
    _analyzer = analyzer;
    _similarity = similarity;
    _batchSize = batchSize;
    _batchDelay = batchDelay;
    _currentZoie = createZoie(_dirMgr);
    _currentZoie.start();
  }
  private ZoieSystem<R, V> createZoie(DirectoryManager dirmgr)
  {
    return new ZoieSystem<R, V>(dirmgr, _interpreter, _decorator, _analyzer, _similarity, _batchSize, _batchDelay, true);
  }
  public Analyzer getAnalyzer()
  {
    return _analyzer;
  }

  public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
  {
    List<ZoieIndexReader<R>> list = new ArrayList<ZoieIndexReader<R>>();
    list.addAll(archiveList);
    if (_oldZoie!=null)
    {
      if(_oldZoie.getCurrentBatchSize()+_oldZoie.getCurrentDiskBatchSize()+_oldZoie.getCurrentMemBatchSize()==0)
      {
        // all events on disk.
        System.out.println(_oldZoie.getAdminMBean().getIndexDir());
        _oldZoie.shutdown();
        IndexReader reader = IndexReader.open(new SimpleFSDirectory(new File(_oldZoie.getAdminMBean().getIndexDir())),true);
        _oldZoie = null;
        ZoieMultiReader<R> zoiereader = new ZoieMultiReader<R>(reader, null);
        archiveList.add(zoiereader);
        list.add(zoiereader);
      } else
      {
        List<ZoieIndexReader<R>> oldlist = _oldZoie.getIndexReaders();
        list.addAll(oldlist);
      }
    }
    List<ZoieIndexReader<R>> readers = _currentZoie.getIndexReaders();
    list.addAll(readers);
    return list;
  }

  public void returnIndexReaders(List<ZoieIndexReader<R>> r)
  {
    _currentZoie.returnIndexReaders(r);
  }

  public void consume(Collection<DataEvent<V>> data)
      throws ZoieException
  {
    // TODO  need to check time boundary. When we hit boundary, we need to trigger DM to 
    // use new dir for zoie and the old one will be archive.
    if (!_dirMgrFactory.updateDirectoryManager())
    {
      _currentZoie.consume(data);
      return;
    }
    // new time period
    _oldZoie = _currentZoie;
    _dirMgr = _dirMgrFactory.getDirectoryManager();
    _dirMgrFactory.clearRecentlyChanged();
    _currentZoie = createZoie(_dirMgr);
    _currentZoie.start();
    _currentZoie.consume(data);
  }
  
  public void shutdown()
  {
    _currentZoie.shutdown();
  }

  public long getVersion()
  {
    return _currentZoie.getVersion();
  }
}
