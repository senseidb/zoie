package proj.zoie.impl.indexing;

import java.util.List;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;

public abstract class AbstractReaderCache<R extends IndexReader> {
  public abstract List<ZoieMultiReader<R>> getIndexReaders();

  public abstract void returnIndexReaders(List<ZoieMultiReader<R>> readers);

  public abstract void refreshCache(long timeout) throws ZoieException;

  public abstract void start();

  public abstract void shutdown();

  public abstract void setFreshness(long freshness);

  public abstract long getFreshness();

}
