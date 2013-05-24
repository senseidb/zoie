package proj.zoie.impl.indexing.internal;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.indexing.IndexReaderDecorator;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <R>
 */
public class DefaultRAMIndexFactory<R extends IndexReader> extends RAMIndexFactory<R> {
  private static final Logger log = Logger.getLogger(DefaultRAMIndexFactory.class);

  @Override
  public synchronized RAMSearchIndex<R> newInstance(String version,
      IndexReaderDecorator<R> decorator, SearchIndexManager<R> idxMgr) {
    return new RAMSearchIndex<R>(version, decorator, idxMgr, new RAMDirectory(), null);
  }
}
