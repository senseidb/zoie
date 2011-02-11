package proj.zoie.impl.indexing;

import java.io.Serializable;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieIndexReader;

public interface ReaderCacheFactory
{
  public <R extends IndexReader, VALUE extends Serializable> AbstractReaderCache<R, VALUE> newInstance(IndexReaderFactory<ZoieIndexReader<R>, VALUE> readerfactory);
}
