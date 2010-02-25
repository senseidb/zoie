package proj.zoie.impl.indexing;
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
import java.io.File;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.DefaultOptimizeScheduler;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.OptimizeScheduler;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.internal.BatchedIndexDataLoader;
import proj.zoie.impl.indexing.internal.DiskLuceneIndexDataLoader;
import proj.zoie.impl.indexing.internal.RealtimeIndexDataLoader;
import proj.zoie.impl.indexing.internal.SearchIndexManager;
import proj.zoie.mbean.ZoieSystemAdminMBean;

/**
 * Zoie system, main class.
 */
public class ZoieSystem<R extends IndexReader,V> extends AsyncDataConsumer<V> implements DataConsumer<V>,IndexReaderFactory<ZoieIndexReader<R>> {

	private static final Logger log = Logger.getLogger(ZoieSystem.class);
	
	private static final int DEFAULT_MAX_BATCH_SIZE = 10000;
	
	private final DirectoryManager _dirMgr;
	private final boolean _realtimeIndexing;
	private final SearchIndexManager<R> _searchIdxMgr;
	private final ZoieIndexableInterpreter<V> _interpreter;
	private final Analyzer _analyzer;
	private final Similarity _similarity;
	private final Queue<IndexingEventListener> _lsnrList;
	private final BatchedIndexDataLoader<R, V> _rtdc;
	private final DiskLuceneIndexDataLoader<R> _diskLoader;
	
	/**
	 * Creates a new ZoieSystem.
	 * @param idxDir index directory, mandatory.
	 * @param interpreter data interpreter, mandatory.
	 * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
	 * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
	 * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
	 * @param batchSize Number of indexing events to hold before flushing to disk.
	 * @param batchDelay How long to wait before flushing to disk.
	 * @param rtIndexing Ensure real-time.
	 */
	public ZoieSystem(File idxDir,ZoieIndexableInterpreter<V> interpreter,IndexReaderDecorator<R> indexReaderDecorator,Analyzer analyzer,Similarity similarity,int batchSize,long batchDelay,boolean rtIndexing)
	{
	  this(new DefaultDirectoryManager(idxDir), interpreter, indexReaderDecorator, analyzer, similarity, batchSize, batchDelay, rtIndexing);
	}
	/**
	 * Creates a new ZoieSystem.
     * @param idxDir index directory, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
     * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
     * @param batchSize Number of indexing events to hold before flushing to disk.
     * @param batchDelay How long to wait before flushing to disk.
     * @param rtIndexing Ensure real-time.
     */
    public ZoieSystem(DirectoryManager dirMgr,ZoieIndexableInterpreter<V> interpreter,IndexReaderDecorator<R> indexReaderDecorator,Analyzer analyzer,Similarity similarity,int batchSize,long batchDelay,boolean rtIndexing)
    {
		if (dirMgr==null) throw new IllegalArgumentException("null directory manager.");
		_dirMgr = dirMgr;
		
		if (interpreter==null) throw new IllegalArgumentException("null interpreter.");

		_searchIdxMgr=new SearchIndexManager<R>(_dirMgr,indexReaderDecorator);
		_realtimeIndexing=rtIndexing;
		_interpreter=interpreter;
		
		_analyzer = analyzer== null ? new StandardAnalyzer(Version.LUCENE_CURRENT) : analyzer;
		_similarity = similarity==null ? new DefaultSimilarity() : similarity;
		
		
		_lsnrList = new ConcurrentLinkedQueue<IndexingEventListener>();
		
		super.setBatchSize(Math.max(1,batchSize)); // realtime memory batch size
		_diskLoader = new DiskLuceneIndexDataLoader<R>(_analyzer, _similarity, _searchIdxMgr);
		_diskLoader.setOptimizeScheduler(new DefaultOptimizeScheduler(getAdminMBean())); // note that the ZoieSystemAdminMBean zoieAdmin parameter for DefaultOptimizeScheduler is not used.
		if (_realtimeIndexing)
		{
		  _rtdc = new RealtimeIndexDataLoader<R, V>(_diskLoader, Math.max(1,batchSize), DEFAULT_MAX_BATCH_SIZE, batchDelay, _analyzer, _similarity, _searchIdxMgr, _interpreter, _lsnrList);
		} else
		{
		  _rtdc = new BatchedIndexDataLoader<R, V>(_diskLoader, Math.max(1,batchSize), DEFAULT_MAX_BATCH_SIZE, batchDelay, _searchIdxMgr, _interpreter, _lsnrList);
		}
		super.setDataConsumer(_rtdc);
		super.setBatchSize(100); // realtime batch size
	}
	
	public static <V> ZoieSystem<IndexReader,V> buildDefaultInstance(File idxDir,ZoieIndexableInterpreter<V> interpreter,int batchSize,long batchDelay,boolean realtime){
		return buildDefaultInstance(idxDir, interpreter, new StandardAnalyzer(Version.LUCENE_CURRENT), new DefaultSimilarity(), batchSize, batchDelay, realtime);
	}
		
	public static <V> ZoieSystem<IndexReader,V> buildDefaultInstance(File idxDir,ZoieIndexableInterpreter<V> interpreter,Analyzer analyzer,Similarity similarity,int batchSize,long batchDelay,boolean realtime){
		return new ZoieSystem<IndexReader,V>(idxDir,interpreter,new DefaultIndexReaderDecorator(),analyzer,similarity,batchSize,batchDelay,realtime);
	}
	
	public void setDocIDMapperFactory(DocIDMapperFactory docIDMapperFactory){
		_searchIdxMgr.setDocIDMapperFactory(docIDMapperFactory);
	}
	
	public void addIndexingEventListener(IndexingEventListener lsnr){
		_lsnrList.add(lsnr);
	}
	
	public OptimizeScheduler getOptimizeScheduler(){
		return _diskLoader.getOptimizeScheduler();
	}
	
	public void setOptimizeScheduler(OptimizeScheduler scheduler){
		if (scheduler!=null){
		  _diskLoader.setOptimizeScheduler(scheduler);
		}
	}
	
	/**
	 * return the current disk version.
	 */
	@Override
	public long getVersion()
	{
	  try{
        return getCurrentDiskVersion();
      } 
      catch (IOException e){
        log.error(e);
      }
      return 0;
	}
	
	public long getCurrentDiskVersion() throws IOException
	{
		return _dirMgr.getVersion();
	}
	
	public Analyzer getAnalyzer()
	{
		return _analyzer;
	}
	
	public Similarity getSimilarity()
	{
		return _similarity;
	}
	
	public void start()
	{
		log.info("starting zoie...");
		_rtdc.start();
        super.start();
		log.info("zoie started...");
	}
	
	public void shutdown()
	{
		log.info("shutting down zoie...");
		_rtdc.shutdown();
        super.stop();
    _searchIdxMgr.close();
		log.info("zoie shutdown successfully.");
		
	}
	
	public void refreshDiskReader() throws IOException
	{
		_searchIdxMgr.refreshDiskReader();
	}
	/**
	 * Flush the memory index into disk.
	 * @throws ZoieException 
	 */
	public void flushEvents(long timeout) throws ZoieException
	{
	  super.flushEvents(timeout);
	  _rtdc.flushEvents(timeout);
	}
	
	public boolean isReadltimeIndexing()
	{
		return _realtimeIndexing;
	}
	
	public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
	{
	  return _searchIdxMgr.getIndexReaders();
	}
	
	public int getDiskSegmentCount() throws IOException{
	  return _searchIdxMgr.getDiskSegmentCount();
	}
	
	public void returnIndexReaders(List<ZoieIndexReader<R>> readers) {
	  try{
        _searchIdxMgr.returnReaders(readers);
      } 
	  catch (IOException e){
		log.error(e.getMessage(),e);
	  }
	}
	
	public void purgeIndex() throws IOException
	{
	  try
	  {
	    flushEvents(20000L);
	  }
	  catch(ZoieException e)
	  {
	  }
	  _searchIdxMgr.purgeIndex();
	}

	public int getCurrentMemBatchSize()
	{
	  return getCurrentBatchSize(); 
	}

	public int getCurrentDiskBatchSize()
	{
	  return _rtdc.getCurrentBatchSize(); 
	}

	public void setMaxBatchSize(int maxBatchSize) {
	  _rtdc.setMaxBatchSize(maxBatchSize);
	}

	public void exportSnapshot(WritableByteChannel channel) throws IOException
	{
	  _diskLoader.exportSnapshot(channel);
	}

	public void importSnapshot(ReadableByteChannel channel) throws IOException
	{
	  _diskLoader.importSnapshot(channel);
	}

	public ZoieSystemAdminMBean getAdminMBean()
	{
		return new MyZoieSystemAdmin();
	}

	private class MyZoieSystemAdmin implements ZoieSystemAdminMBean
	{
	  public void refreshDiskReader() throws IOException{
	    ZoieSystem.this.refreshDiskReader();
	  }

	  public long getBatchDelay() {
	    return _rtdc.getDelay();
	  }

	  public int getBatchSize() {
	    return _rtdc.getBatchSize();
	  }

	  public long getCurrentDiskVersion() throws IOException
	  {
	    return ZoieSystem.this.getCurrentDiskVersion();
	  }

	  public int getDiskIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getDiskIndexSize();
	  }

	  public String getDiskIndexerStatus() {
	    return String.valueOf(ZoieSystem.this._searchIdxMgr.getDiskIndexerStatus());
	  }

	  public Date getLastDiskIndexModifiedTime() {
	    return ZoieSystem.this._dirMgr.getLastIndexModifiedTime();
	  }

	  public String getIndexDir()
	  {
	    return ZoieSystem.this._dirMgr.getPath();
	  }

	  public Date getLastOptimizationTime() {
	    return new Date(_diskLoader.getLastTimeOptimized());
	  }

	  public int getMaxBatchSize() {
	    return _rtdc.getMaxBatchSize();
	  }


	  public int getDiskIndexSegmentCount() throws IOException{
	    return ZoieSystem.this.getDiskSegmentCount();
	  }

	  public boolean isRealtime(){
	    return ZoieSystem.this.isReadltimeIndexing();
	  }

	  public int getRamAIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getRamAIndexSize();
	  }

	  public long getRamAVersion() {
	    return ZoieSystem.this._searchIdxMgr.getRamAVersion();
	  }

	  public int getRamBIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getRamBIndexSize();
	  }

	  public long getRamBVersion() {
	    return ZoieSystem.this._searchIdxMgr.getRamBVersion();
	  }

	  public void optimize(int numSegs) throws IOException {
	    _diskLoader.optimize(numSegs);
	  }

	  public void flushToDiskIndex() throws ZoieException
	  {
	    log.info("flushing to disk");
	    ZoieSystem.this.flushEvents(Long.MAX_VALUE);
	    log.info("all events flushed to disk");
	  }

	  public void setBatchDelay(long batchDelay) {
	    _rtdc.setDelay(batchDelay);
	  }

	  public void setBatchSize(int batchSize) {
	    _rtdc.setBatchSize(batchSize);
	  }

	  public void setMaxBatchSize(int maxBatchSize) {
	    ZoieSystem.this.setMaxBatchSize(maxBatchSize);
	  }

	  public void purgeIndex() throws IOException{
	    ZoieSystem.this.purgeIndex();
	  }

	  public void expungeDeletes() throws IOException{
	    _diskLoader.expungeDeletes();
	  }

	  public void setNumLargeSegments(int numLargeSegments)
	  {
	    ZoieSystem.this._searchIdxMgr.setNumLargeSegments(numLargeSegments);
	  }

	  public int getNumLargeSegments()
	  {
	    return ZoieSystem.this._searchIdxMgr.getNumLargeSegments();
	  }

	  public void setMaxSmallSegments(int maxSmallSegments)
	  {
	    ZoieSystem.this._searchIdxMgr.setMaxSmallSegments(maxSmallSegments);
	  }

	  public int getMaxSmallSegments()
	  {
	    return ZoieSystem.this._searchIdxMgr.getMaxSmallSegments();
	  }

	  public int getMaxMergeDocs() {
	    return ZoieSystem.this._searchIdxMgr.getMaxMergeDocs();
	  }

	  public int getMergeFactor() {
	    return ZoieSystem.this._searchIdxMgr.getMergeFactor();
	  }

	  public void setMaxMergeDocs(int maxMergeDocs) {
	    ZoieSystem.this._searchIdxMgr.setMaxMergeDocs(maxMergeDocs);
	  }

	  public void setMergeFactor(int mergeFactor) {
	    ZoieSystem.this._searchIdxMgr.setMergeFactor(mergeFactor);
	  }

	  public boolean isUseCompoundFile() {
	    return ZoieSystem.this._searchIdxMgr.isUseCompoundFile();
	  }

	  public void setUseCompoundFile(boolean useCompoundFile) {
	    ZoieSystem.this._searchIdxMgr.setUseCompoundFile(useCompoundFile);
	  }

	  public int getCurrentMemBatchSize()
	  {
	    return ZoieSystem.this.getCurrentMemBatchSize(); 
	  }

	  public int getCurrentDiskBatchSize()
	  {
	    return ZoieSystem.this.getCurrentDiskBatchSize(); 
	  }
	}
}
