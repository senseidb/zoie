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
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.impl.ZoieMergePolicy.MergePolicyParams;
import proj.zoie.api.impl.util.IndexUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DiskSearchIndex<R extends IndexReader> extends BaseSearchIndex<R>{
  private final DirectoryManager        _dirMgr;
  private final IndexReaderDispenser<R> _dispenser;

  final MergePolicyParams _mergePolicyParams;

  private ZoieIndexDeletionPolicy _deletionPolicy;

  public static final Logger log = Logger.getLogger(DiskSearchIndex.class);

  DiskSearchIndex(DirectoryManager dirMgr, IndexReaderDecorator<R> decorator,SearchIndexManager<R> idxMgr){
    super(idxMgr);  
    _dirMgr = dirMgr;
    _mergePolicyParams = new MergePolicyParams();
    _dispenser = new IndexReaderDispenser<R>(_dirMgr, decorator,this);
    _mergeScheduler = new SerialMergeScheduler();
    _deletionPolicy = new ZoieIndexDeletionPolicy();
  }

  public long getVersion()
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

  /**
   * Close and releases dispenser and clean up
   */
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
        log.error(e.getMessage(),e);
      }
    }
  }

  public void returnReaders(List<ZoieIndexReader<R>> readers) throws IOException
  {
    _dispenser.returnReaders(readers);
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
  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity) throws IOException
  {
    if(_indexWriter != null) return _indexWriter;

    Directory directory = _dirMgr.getDirectory(true);

    log.info("opening index writer at: "+_dirMgr.getPath());

    // create a new modifier to the index, assuming at most one instance is running at any given time
    boolean create = !IndexReader.indexExists(directory);  
    IndexWriter idxWriter = new IndexWriter(directory, analyzer, create, _deletionPolicy, MaxFieldLength.UNLIMITED);
    idxWriter.setMergeScheduler(_mergeScheduler);

    ZoieMergePolicy mergePolicy = new ZoieMergePolicy(idxWriter);
    mergePolicy.setMergePolicyParams(_mergePolicyParams);
    idxWriter.setRAMBufferSizeMB(5);

    idxWriter.setMergePolicy(mergePolicy);

    if (similarity != null)
    {
      idxWriter.setSimilarity(similarity);
    }
    _indexWriter = idxWriter;
    return idxWriter;
  }

  /**
   * Gets the current reader
   */
  public ZoieIndexReader<R> openIndexReader() throws IOException
  {
    // use dispenser to get the reader
    return _dispenser.getIndexReader();

  }


  @Override
  protected IndexReader openIndexReaderForDelete() throws IOException {
    Directory directory = _dirMgr.getDirectory(true);
    if (IndexReader.indexExists(directory)){		
      return IndexReader.open(directory,false);
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
  public void setVersion(long version) throws IOException
  {
    _dirMgr.setVersion(version);
  }

  public DiskIndexSnapshot getSnapshot()
  {
    IndexSignature sig = _dirMgr.getCurrentIndexSignature();
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
