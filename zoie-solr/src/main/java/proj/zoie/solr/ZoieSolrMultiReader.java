package proj.zoie.solr;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

public class ZoieSolrMultiReader<T extends IndexReader> extends MultiReader {

  private ZoieIndexReader<T> _diskReader = null;
  private final IndexReaderFactory<ZoieIndexReader<T>> _readerFactory;
  private final List<ZoieIndexReader<T>> _subReaders;

  public ZoieSolrMultiReader(List<ZoieIndexReader<T>> subReaders,
      IndexReaderFactory<ZoieIndexReader<T>> readerFactory) {
    super(subReaders.toArray(new ZoieIndexReader[subReaders.size()]), false);
    _subReaders = subReaders;
    _readerFactory = readerFactory;
    for (ZoieIndexReader<T> subReader : subReaders) {
      Directory dir = subReader.directory();
      if (dir instanceof FSDirectory) {
        _diskReader = subReader;
        break;
      }
    }
  }

  @Override
  public Directory directory() {
    return _diskReader.directory();
  }

  @Override
  public long getVersion() {
    return _diskReader.getVersion();
  }

  @Override
  public IndexCommit getIndexCommit() throws IOException {
    Directory dir = _diskReader.directory();
    SegmentInfos segmentInfos = new SegmentInfos();
    segmentInfos.read(dir);
    return new ZoieSolrIndexCommit(segmentInfos, dir);
  }

  @Override
  public synchronized IndexReader reopen() throws CorruptIndexException, IOException {
    return reopen(true);
  }

  @Override
  public synchronized IndexReader reopen(boolean openReadOnly) throws CorruptIndexException,
      IOException {
    if (!openReadOnly) {
      throw new IllegalStateException("Zoie readers must be read-only");
    }
    List<ZoieIndexReader<T>> readerList = _readerFactory.getIndexReaders();
    IndexReader retReader = new ZoieSolrMultiReader<T>(readerList, _readerFactory);
    _readerFactory.returnIndexReaders(_subReaders);
    return retReader;
  }

  @Override
  public synchronized IndexReader reopen(IndexCommit commit) throws CorruptIndexException,
      IOException {
    return reopen(true);
  }
}
