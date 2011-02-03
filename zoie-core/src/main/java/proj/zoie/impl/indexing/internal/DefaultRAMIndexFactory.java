package proj.zoie.impl.indexing.internal;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.indexing.IndexReaderDecorator;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <R>
 */
public class DefaultRAMIndexFactory<R extends IndexReader, V extends ZoieVersion> extends RAMIndexFactory<R, V>
{
  private static final Logger log = Logger.getLogger(DefaultRAMIndexFactory.class);

  @Override
  public synchronized RAMSearchIndex<R, V> newInstance(V version, IndexReaderDecorator<R> decorator, SearchIndexManager<R, V, ?> idxMgr)
  {
    return new RAMSearchIndex<R, V>(version, decorator, idxMgr, new RAMDirectory(), null);
  }
}
