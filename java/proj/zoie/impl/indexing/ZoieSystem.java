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
import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieVersionFactory;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.DefaultDocIDMapperFactory;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.indexing.DefaultOptimizeScheduler;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.IndexingEventListener;
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

public class ZoieSystem<R extends IndexReader,D, V extends ZoieVersion> extends AsyncDataConsumer<D,V> implements DataConsumer<D,V>,IndexReaderFactory<ZoieIndexReader<R>>, ZoieVersionFactory<V>{

	private static final Logger log = Logger.getLogger(ZoieSystem.class);
	
	private final DirectoryManager<V> _dirMgr;
	private final boolean _realtimeIndexing;
	private final SearchIndexManager<R,V> _searchIdxMgr;
	private final ZoieIndexableInterpreter<D> _interpreter;
	private final Analyzer _analyzer;
	private final Similarity _similarity;

	private final Queue<IndexingEventListener<V>> _lsnrList;
	private final BatchedIndexDataLoader<R,D,V> _rtdc;
	private final DiskLuceneIndexDataLoader<R,V> _diskLoader;
	private volatile boolean alreadyShutdown = false;

	
	 
  
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
  public ZoieSystem(File idxDir, 
                            ZoieIndexableInterpreter<D> interpreter,
                            IndexReaderDecorator<R> indexReaderDecorator,
                            Analyzer analyzer,
                            Similarity similarity,
                            int batchSize,long batchDelay,boolean rtIndexing,
                            ZoieVersionFactory<V> zoieVersionFactory)
  {
    this(new DefaultDirectoryManager<V>(idxDir,zoieVersionFactory), interpreter, indexReaderDecorator, analyzer, similarity, batchSize, batchDelay, rtIndexing,zoieVersionFactory);
  }
  
  
	/**
	 * Creates a new ZoieSystem.
     * @param dirMgr Directory manager, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param zoieConfig configuration object
     */
    public ZoieSystem(DirectoryManager<V> dirMgr,
                             ZoieIndexableInterpreter<D> interpreter,
                             IndexReaderDecorator<R> indexReaderDecorator,
                             ZoieConfig<V> zoieConfig){
    	this(dirMgr, interpreter, indexReaderDecorator, zoieConfig.getDocidMapperFactory(),  zoieConfig.getAnalyzer(),
    	     zoieConfig.getSimilarity(), zoieConfig.getBatchSize(), zoieConfig.getBatchDelay(), zoieConfig.isRtIndexing(), zoieConfig.getMaxBatchSize(),zoieConfig.getZoieVersionFactory());
    }

    /**
	 * Creates a new ZoieSystem.
     * @param idxDir index directory, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param zoieConfig configuration object
     */
    public ZoieSystem(File idxDir,
                             ZoieIndexableInterpreter<D> interpreter,
                             IndexReaderDecorator<R> indexReaderDecorator,
                             ZoieConfig<V> zoieConfig){
    	this(new DefaultDirectoryManager<V>(idxDir,zoieConfig.getZoieVersionFactory()),interpreter,indexReaderDecorator,zoieConfig.getDocidMapperFactory(),zoieConfig.getAnalyzer(),
    	     zoieConfig.getSimilarity(),zoieConfig.getBatchSize(),zoieConfig.getBatchDelay(),zoieConfig.isRtIndexing(),zoieConfig.getMaxBatchSize(),zoieConfig.getZoieVersionFactory());
    }
    
    
    /**
     * Creates a new ZoieSystem.
       * @param dirMgr Directory manager, mandatory.
       * @param interpreter data interpreter, mandatory.
       * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
       * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
       * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
       * @param batchSize Number of indexing events to hold before flushing to disk.
       * @param batchDelay How long to wait before flushing to disk.
       * @param rtIndexing Ensure real-time.
       */
      public ZoieSystem(DirectoryManager<V> dirMgr,
                                ZoieIndexableInterpreter<D> interpreter,
                                IndexReaderDecorator<R> indexReaderDecorator,
                                Analyzer analyzer,
                                Similarity similarity,
                                int batchSize,long batchDelay,boolean rtIndexing,
                                ZoieVersionFactory<V> zoieVersionFactory)
      {
        this(dirMgr, interpreter, indexReaderDecorator, new DefaultDocIDMapperFactory(),  analyzer, similarity, batchSize, batchDelay, rtIndexing, zoieVersionFactory);
       // this(dirMgr, interpreter, indexReaderDecorator, analyzer, similarity, batchSize, batchDelay, rtIndexing);
      }
    
      
    /**
     * Creates a new ZoieSystem.
     * @param dirMgr Directory manager, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param docIdMapperFactory custom docid mapper factory
     * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
     * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
     * @param batchSize desired number of indexing events to hold in buffer before indexing. If we already have this many, we hold back the data provider.
     * @param batchDelay How long to wait before flushing to disk.
     * @param rtIndexing Ensure real-time.
     */
    public ZoieSystem(DirectoryManager<V> dirMgr,
                              ZoieIndexableInterpreter<D> interpreter,
                              IndexReaderDecorator<R> indexReaderDecorator,
                              DocIDMapperFactory docidMapperFactory, 
                              Analyzer analyzer,
                              Similarity similarity,
                              int batchSize,long batchDelay,boolean rtIndexing,
                              ZoieVersionFactory<V> zoieVersionFactory){
    	this(dirMgr,interpreter,indexReaderDecorator,docidMapperFactory, analyzer,similarity,batchSize,batchDelay,rtIndexing,ZoieConfig.DEFAULT_MAX_BATCH_SIZE,zoieVersionFactory);
    }
    
    /**
     * Creates a new ZoieSystem.
     * @param idxDir index directory, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param docIdMapperFactory custom docid mapper factory
     * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
     * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
     * @param batchSize Number of indexing events to hold before flushing to disk.
     * @param batchDelay How long to wait before flushing to disk.
     * @param rtIndexing Ensure real-time.
     */
    public ZoieSystem(File idxDir,
                              ZoieIndexableInterpreter<D> interpreter,
                              IndexReaderDecorator<R> indexReaderDecorator,
                              DocIDMapperFactory docIdMapperFactory, 
                              Analyzer analyzer, 
                              Similarity similarity,
                              int batchSize,long batchDelay,boolean rtIndexing,
                              ZoieVersionFactory<V> zoieVersionFactory)
    {
      this(new DefaultDirectoryManager<V>(idxDir,zoieVersionFactory), interpreter, indexReaderDecorator, docIdMapperFactory, analyzer, similarity, batchSize, batchDelay, rtIndexing,zoieVersionFactory);
    }
    
    
    
    /**
     * Creates a new ZoieSystem.
     * @param dirMgr Directory manager, mandatory.
     * @param interpreter data interpreter, mandatory.
     * @param indexReaderDecorator index reader decorator,optional. If not specified, {@link proj.zoie.impl.indexing.DefaultIndexReaderDecorator} is used. 
     * @param docIdMapperFactory custom docid mapper factory
     * @param analyzer Default analyzer, optional. If not specified, {@link org.apache.lucene.analysis.StandardAnalyzer} is used.
     * @param similarity Default similarity, optional. If not specified, {@link org.apache.lucene.search.DefaultSimilarity} is used.
     * @param batchSize desired number of indexing events to hold in buffer before indexing. If we already have this many, we hold back the data provider.
     * @param batchDelay How long to wait before flushing to disk.
     * @param rtIndexing Ensure real-time.
     * @param maxBatchSize maximum batch size
     */
    public ZoieSystem(DirectoryManager<V> dirMgr,
                             ZoieIndexableInterpreter<D> interpreter,
                             IndexReaderDecorator<R> indexReaderDecorator,
                             DocIDMapperFactory docidMapperFactory,
                             Analyzer analyzer,
                             Similarity similarity,
                             int batchSize,long batchDelay,boolean rtIndexing,
                             int maxBatchSize,
                             ZoieVersionFactory<V> zoieVersionFactory)
    {
      if (dirMgr==null) throw new IllegalArgumentException("null directory manager.");
      _dirMgr = dirMgr;

      if (interpreter==null) throw new IllegalArgumentException("null interpreter.");

      docidMapperFactory = docidMapperFactory==null ? new DefaultDocIDMapperFactory() : docidMapperFactory;
      _searchIdxMgr=new SearchIndexManager<R,V>(_dirMgr,indexReaderDecorator,docidMapperFactory,zoieVersionFactory);
      _realtimeIndexing=rtIndexing;
      _interpreter=interpreter;

      _analyzer = analyzer== null ? new StandardAnalyzer(Version.LUCENE_CURRENT) : analyzer;
      _similarity = similarity==null ? new DefaultSimilarity() : similarity;
      log.info("creating Zoie instance --> "
          + _dirMgr.toString()
          + "\t" + _interpreter.toString()
          + "\t" + (indexReaderDecorator!=null?indexReaderDecorator.toString():"null")
          + "\t" + docidMapperFactory.toString()
          + "\t" + zoieVersionFactory.toString()
          + "\tAnalyzer: " + _analyzer.toString()
          + "\tSimilarity: " + _similarity.toString()
          + "\tbatchSize (desired max batch size for indexing to RAM): " + batchSize
          + "\tbatchDelay (max time to wait before flushing to disk): " + batchDelay
          + "\trealtime mode: " + rtIndexing);


      _lsnrList = new ConcurrentLinkedQueue<IndexingEventListener<V>>();

      super.setBatchSize(Math.max(1,batchSize)); // realtime memory batch size
      _diskLoader = new DiskLuceneIndexDataLoader<R,V>(_analyzer, _similarity, _searchIdxMgr);
      _diskLoader.setOptimizeScheduler(new DefaultOptimizeScheduler(getAdminMBean())); // note that the ZoieSystemAdminMBean zoieAdmin parameter for DefaultOptimizeScheduler is not used.
      batchSize = Math.max(1, batchSize);
      if (_realtimeIndexing)
      {
        _rtdc = new RealtimeIndexDataLoader<R,D,V>(_diskLoader, batchSize, Math.max(batchSize, maxBatchSize), batchDelay, _analyzer, _similarity, _searchIdxMgr, _interpreter, _lsnrList);
      } else
      {
        _rtdc = new BatchedIndexDataLoader<R,D,V>(_diskLoader, batchSize, Math.max(batchSize, maxBatchSize), batchDelay, _searchIdxMgr, _interpreter, _lsnrList);
      }
      super.setDataConsumer(_rtdc);
      super.setBatchSize(100); // realtime batch size
    }

    public static <D,V extends ZoieVersion> ZoieSystem<IndexReader,D,V> buildDefaultInstance(File idxDir,ZoieIndexableInterpreter<D> interpreter,int batchSize,long batchDelay,boolean realtime,ZoieVersionFactory<V> zoieVersionFactory){
      return buildDefaultInstance(idxDir, interpreter, new StandardAnalyzer(Version.LUCENE_CURRENT), new DefaultSimilarity(), batchSize, batchDelay, realtime,zoieVersionFactory);
    }

    public static <D,V extends ZoieVersion> ZoieSystem<IndexReader,D,V> buildDefaultInstance(File idxDir,ZoieIndexableInterpreter<D> interpreter,Analyzer analyzer,Similarity similarity,int batchSize,long batchDelay,boolean realtime,ZoieVersionFactory<V> zoieVersionFactory){
      return new ZoieSystem<IndexReader,D,V>(idxDir,interpreter,new DefaultIndexReaderDecorator(),analyzer,similarity,batchSize,batchDelay,realtime,zoieVersionFactory);
    }

    public void addIndexingEventListener(IndexingEventListener<V> lsnr){
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
   * return the minimum zoie version.
   */
  //@Override
  //public V getMinZoieVersion()
  //{
  //  return null;
  //}
  
  /**
   * return the zoie version given a string.
   */
  @Override
  public V getZoieVersion(String str)
  {
    return getVersion();
  }
  
  //public V nextZoieVersion()
  //{
  //  return null;
 // }
  
	/**
	 * return the current disk version.
	 */
	@Override
	public V getVersion()
	{
	  try{
        return getCurrentDiskVersion();
      } 
      catch (IOException e){
        log.error(e);
      }
      
      return null;
	}
	
	public V getCurrentDiskVersion() throws IOException
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
		try
    {
      flushEvents(200000);
    } catch (ZoieException e)
    {
      log.error("zoie shutdown encountered ", e);
    }
		_rtdc.shutdown();
    super.stop();
    _searchIdxMgr.close();
		log.info("zoie shutdown successfully.");
		alreadyShutdown = true;
	}
	public boolean alreadyShutdown()
	{
	  return alreadyShutdown;
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
	
    /**
     * Flush events to the memory index.
     * @throws ZoieException 
     */
    public void flushEventsToMemoryIndex(long timeout) throws ZoieException
    {
      super.flushEvents(timeout);
    }
    
	public boolean isReadltimeIndexing()
	{
		return _realtimeIndexing;
	}
	
	/**
   * return a list of ZoieIndexReaders. These readers are reference counted and this method
   * should be used in pair with returnIndexReaders(List<ZoieIndexReader<R>> readers) {@link #returnIndexReaders(List)}.
   * It is typical that we create a MultiReader from these readers. When creating MultiReader, it should be created with
   * the closeSubReaders parameter set to false in order to do reference counting correctly.
	 * @see proj.zoie.api.IndexReaderFactory#getIndexReaders()
	 * @see proj.zoie.impl.indexing.ZoieSystem#returnIndexReaders(List)
	 */
	public List<ZoieIndexReader<R>> getIndexReaders() throws IOException
	{
	  return _searchIdxMgr.getIndexReaders();
	}
	
	public int getDiskSegmentCount() throws IOException{
	  return _searchIdxMgr.getDiskSegmentCount();
	}
  public int getRAMASegmentCount()
  {
    return _searchIdxMgr.getRAMASegmentCount();
  }
  public int getRAMBSegmentCount()
  {
    return _searchIdxMgr.getRAMBSegmentCount();
  }

	/**
   * return the index readers. Since Zoie reuse the index readers, the reference counting
   * is centralized. Same readers should not be returned more than once.
	 * @param readers The index readers to return. Should be the same as the one obtained from
	 * calling getIndexReaders()
	 * 
   * @see proj.zoie.impl.indexing.ZoieSystem#getIndexReaders()
	 * @see proj.zoie.api.IndexReaderFactory#returnIndexReaders(java.util.List)
	 */
	public void returnIndexReaders(List<ZoieIndexReader<R>> readers)
	{
	  for(ZoieIndexReader<R> r : readers)
	  {
	    try
      {
        r.decRef();
      } catch (IOException e)
      {
        log.error("error when decRef on reader ", e);
      }
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
	
    public long getMinUID() throws IOException
    {
      long minUID = Long.MAX_VALUE;
      List<ZoieIndexReader<R>> readers = getIndexReaders();
      try
      {
        for(ZoieIndexReader<R> reader : readers)
        {
          long uid = reader.getMinUID();
          minUID = (uid < minUID ? uid : minUID);
        }
        return minUID;
      }
      finally
      {
        returnIndexReaders(readers);
      }
    }

    public long getMaxUID() throws IOException
    {
      long maxUID = Long.MIN_VALUE;
      List<ZoieIndexReader<R>> readers = getIndexReaders();
      try
      {
        for(ZoieIndexReader<R> reader : readers)
        {
          long uid = reader.getMaxUID();
          maxUID = (uid > maxUID ? uid : maxUID);
        }
        return maxUID;
      }
      finally
      {
        returnIndexReaders(readers);
      }
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

	  public V getCurrentDiskVersion() throws IOException
	  {
	    return ZoieSystem.this.getCurrentDiskVersion();
	  }

	  public int getDiskIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getDiskIndexSize();
	  }

    public long getDiskIndexSizeBytes()
    {
      return FileUtil.sizeFile(new File(getIndexDir()));
    }

    /*
     * (non-Javadoc)
     * 
     * @see proj.zoie.mbean.ZoieSystemAdminMBean#getDiskFreeSpaceBytes()
     */
    public long getDiskFreeSpaceBytes()
    {
      File index = new File(getIndexDir());
      if (!index.exists())
        return -1;
      return index.getUsableSpace();
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

    public int getRAMASegmentCount()
    {
      return ZoieSystem.this.getRAMASegmentCount();
    }

    public int getRAMBSegmentCount()
    {
      return ZoieSystem.this.getRAMBSegmentCount();
    }

	  public boolean isRealtime(){
	    return ZoieSystem.this.isReadltimeIndexing();
	  }

	  public int getRamAIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getRamAIndexSize();
	  }

	  public V getRamAVersion() {
	    return ZoieSystem.this._searchIdxMgr.getRamAVersion();
	  }

	  public int getRamBIndexSize() {
	    return ZoieSystem.this._searchIdxMgr.getRamBIndexSize();
	  }

	  public V getRamBVersion() {
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
	  
      public void flushToMemoryIndex() throws ZoieException
      {
        log.info("flushing to memory");
        ZoieSystem.this.flushEventsToMemoryIndex(Long.MAX_VALUE);
        log.info("all events flushed to memory");
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
	  
      public long getMinUID() throws IOException
      {
        return ZoieSystem.this.getMinUID();
      }
      
      public long getMaxUID() throws IOException
      {
        return ZoieSystem.this.getMaxUID();
      }
	}
}
