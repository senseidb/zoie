package proj.zoie.impl.indexing;

import java.io.Serializable;
import java.util.List;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;

public abstract class AbstractReaderCache<R extends IndexReader, VALUE extends Serializable>
{
  public abstract List<ZoieIndexReader<R>> getIndexReaders();

  public abstract void returnIndexReaders(List<ZoieIndexReader<R>> readers);

  public abstract void refreshCache(long timeout) throws ZoieException;

  public abstract void start();

  public abstract void shutdown();

  public abstract void setFreshness(long freshness);

  public abstract long getFreshness();
  
}
