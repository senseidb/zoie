package proj.zoie.impl.indexing.internal;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.indexing.IndexReaderDecorator;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 */
public abstract class RAMIndexFactory<R extends IndexReader, V extends ZoieVersion>
{
  public abstract RAMSearchIndex<R, V> newInstance(V version, IndexReaderDecorator<R> decorator, SearchIndexManager<R, V> idxMgr);
}
