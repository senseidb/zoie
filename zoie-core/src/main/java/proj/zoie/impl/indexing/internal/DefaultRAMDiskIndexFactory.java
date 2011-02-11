package proj.zoie.impl.indexing.internal;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.indexing.IndexReaderDecorator;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 * @param <R>
 */
public class DefaultRAMDiskIndexFactory<R extends IndexReader, V extends ZoieVersion> extends RAMIndexFactory<R, V>
{
  private static final Logger log = Logger.getLogger(DefaultRAMIndexFactory.class);
  private static int fold = 10000;

  @Override
  public synchronized RAMSearchIndex<R, V> newInstance(V version, IndexReaderDecorator<R> decorator, SearchIndexManager<R, V, ?> idxMgr)
  {
    Directory ramIdxDir;
    try
    {
      File backingdir = new File("/tmp/ram" + fold);// /Volumes/ramdisk/
      ramIdxDir = new SimpleFSDirectory(backingdir);
      fold++;
      return new RAMSearchIndex<R, V>(version, decorator, idxMgr, ramIdxDir, backingdir);
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      log.error(e);
      e.printStackTrace();
    }// new RAMDirectory();
    return null;
  }

}
