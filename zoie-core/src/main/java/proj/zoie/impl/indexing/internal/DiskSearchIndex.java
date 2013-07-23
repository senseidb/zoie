package proj.zoie.impl.indexing.internal;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.impl.ZoieMergePolicy.MergePolicyParams;
import proj.zoie.api.impl.util.IndexUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DiskSearchIndex<R extends IndexReader> extends BaseSearchIndex<R>{
  private final DirectoryManager        _dirMgr;
  private final IndexReaderDispenser<R> _dispenser;

  final MergePolicyParams _mergePolicyParams;

  private final ZoieIndexDeletionPolicy _deletionPolicy;

  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);


  public DiskSearchIndex(DirectoryManager dirMgr, IndexReaderDecorator<R> decorator,SearchIndexManager<R> idxMgr){
    super(idxMgr, true);
    _dirMgr = dirMgr;
    _mergePolicyParams = new MergePolicyParams();
    _dispenser = new IndexReaderDispenser<R>(_dirMgr, decorator,this);
    _mergeScheduler = new SerialMergeScheduler();
    _deletionPolicy = new ZoieIndexDeletionPolicy();
  }

  @Override
  public String getVersion()
  {
    return _dispenser.getCurrentVersion();
  }

  public MergePolicyParams getMergePolicyParams(){
    return _mergePolicyParams;
  }

  /**
   * Gets the number of docs in the current loaded index
   * @return number of docs
   */
  @Override
  public int getNumdocs()
  {
    IndexReader reader=_dispenser.getIndexReader();
    if (reader!=null)
    {
      return reader.numDocs();
    }
    else
    {
      return 0;
    }
  }

  public int getSegmentCount() throws IOException{
    if (_dirMgr == null || !_dirMgr.exists()){
      return 0;
    }

    Directory dir = null;
    try{
      dir = _dirMgr.getDirectory();
    }
    catch(Exception e){
      return 0;
    }
    if (dir == null) return 0;
    return IndexUtil.getNumSegments(dir);
  }
  public String getSegmentInfo()
  {
    if (_dirMgr == null || !_dirMgr.exists())
    {
      return "";
    }

    Directory dir = null;
    try
    {
      dir = _dirMgr.getDirectory();
    }
    catch(Exception e)
    {
      return "";
    }
    if (dir == null) return "";
    return IndexUtil.getSegmentsInfo(dir);
  }

  /**
   * Close and releases dispenser and clean up
   */
  @Override
  public void close()
  {
    super.close();

    // close the dispenser
    if (_dispenser != null)
    {
      _dispenser.close();
    }
  }

  /**
   * Refreshes the index reader
   */
  @Override
  public void refresh()
  {
    synchronized(this)
    {
      try {
        LongSet delDocs = _delDocs;
        clearDeletes();
        _dispenser.getNewReader();
        markDeletes(delDocs); // re-mark deletes
        commitDeletes();
      } catch (IOException e) {
        ZoieHealth.setFatal();
        log.error(e.getMessage(),e);
      }
      finally{
        synchronized(readerOpenLock){
          readerOpenLock.notifyAll();
        }
      }
    }
  }

	  @Override
	  protected void finalize()
	  {
	    close();
	  }


  /**
   * Opens an index modifier.
   * @param analyzer Analyzer
   * @return IndexModifer instance
   */
  @Override
  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException
  {
    if(_indexWriter != null) return _indexWriter;

    Directory directory = _dirMgr.getDirectory(true);

    log.info("opening index writer at: "+_dirMgr.getPath());

    ZoieMergePolicy mergePolicy = new ZoieMergePolicy();
    mergePolicy.setMergePolicyParams(_mergePolicyParams);

    // hao: autocommit is set to false with this constructor
    IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_34,analyzer);
    config.setOpenMode(OpenMode.CREATE_OR_APPEND);
    config.setIndexDeletionPolicy(_deletionPolicy);
    config.setMergeScheduler(_mergeScheduler);
    config.setMergePolicy(mergePolicy);
    config.setReaderPooling(false);
    if (similarity!=null){
      config.setSimilarity(similarity);
    }
    config.setRAMBufferSizeMB(5);
    IndexWriter idxWriter = new IndexWriter(directory,config);

    _indexWriter = idxWriter;
    return idxWriter;
  }

  /**
   * Gets the current reader
   */
  @Override
  public ZoieIndexReader<R> openIndexReader()
  {
    // use dispenser to get the reader
    return _dispenser.getIndexReader();
  }

  private final Object readerOpenLock = new Object();

  public ZoieIndexReader<R> openIndexReader(String minVersion,long timeout) throws IOException,TimeoutException{
    if (_versionComparator.compare(minVersion,_dispenser.getCurrentVersion())<=0){
      return _dispenser.getIndexReader();
    }
    long start = System.currentTimeMillis();

    while(_versionComparator.compare(minVersion, _dispenser.getCurrentVersion())>0){
      synchronized(readerOpenLock){
        try {
          readerOpenLock.wait(100);
        } catch (InterruptedException e) {
          // ignore
        }
      }
      long now = System.currentTimeMillis();
      if (now-start>=timeout) throw new TimeoutException("timed-out, took: "+(now-start)+" ms");
    }

    return _dispenser.getIndexReader();

  }


  @Override
  protected IndexReader openIndexReaderForDelete() throws IOException {
    Directory directory = _dirMgr.getDirectory(true);
    if (IndexReader.indexExists(directory)){
      return IndexReader.open(directory, _deletionPolicy, false);
    }
    else{
      return null;
    }
  }

  /**
   * Gets a new reader, force a reader refresh
   * @return
   * @throws IOException
   */
  public ZoieIndexReader<R> getNewReader() throws IOException
  {
    synchronized(this)
    {
      refresh();
      commitDeletes();
      ZoieIndexReader<R> reader = _dispenser.getIndexReader();
      return reader;
    }
  }

  /**
   * Writes the current version/SCN to the disk
   */
  @Override
  public void setVersion(String version) throws IOException
  {
    _dirMgr.setVersion(version);
  }

  public DiskIndexSnapshot getSnapshot() throws IOException
  {
    IndexSignature sig = new IndexSignature(_dirMgr.getVersion());
    if(sig != null)
    {
      ZoieIndexDeletionPolicy.Snapshot snapshot = _deletionPolicy.getSnapshot();
      if(snapshot != null)
      {
        return new DiskIndexSnapshot(_dirMgr, sig, snapshot);
      }
    }
    return null;
  }

  public void importSnapshot(ReadableByteChannel channel) throws IOException
  {
    DiskIndexSnapshot.readSnapshot(channel, _dirMgr);
  }
}
