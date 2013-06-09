package proj.zoie.impl.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;

public class SimpleReaderCache<R extends IndexReader> extends AbstractReaderCache<R> {

  private static final Logger logger = Logger.getLogger(SimpleReaderCache.class);

  private final IndexReaderFactory<R> _readerFactory;

  public SimpleReaderCache(IndexReaderFactory<R> readerfactory) {
    _readerFactory = readerfactory;
  }

  @Override
  public List<ZoieMultiReader<R>> getIndexReaders() {
    try {
      return _readerFactory.getIndexReaders();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return new ArrayList<ZoieMultiReader<R>>();
    }
  }

  @Override
  public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
    _readerFactory.returnIndexReaders(readers);
  }

  @Override
  public void refreshCache(long timeout) throws ZoieException {

  }

  @Override
  public void start() {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void setFreshness(long freshness) {

  }

  @Override
  public long getFreshness() {
    return 0;
  }

  public static ReaderCacheFactory FACTORY = new ReaderCacheFactory() {

    @Override
    public <R extends IndexReader> AbstractReaderCache<R> newInstance(
        IndexReaderFactory<R> readerfactory) {
      return new SimpleReaderCache<R>(readerfactory);
    }
  };

}
