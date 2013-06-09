package proj.zoie.impl.indexing;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;

public interface ReaderCacheFactory {
  public <R extends IndexReader> AbstractReaderCache<R> newInstance(
      IndexReaderFactory<R> readerfactory);
}
