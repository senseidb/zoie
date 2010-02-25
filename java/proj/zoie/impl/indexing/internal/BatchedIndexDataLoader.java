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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.IndexUpdatedEvent;
import proj.zoie.impl.indexing.IndexingEventListener;
import proj.zoie.impl.indexing.IndexingThread;
import proj.zoie.impl.indexing.IndexingEventListener.IndexingEvent;

/**
 * Runs a background thread that flushes incoming data events in batch to the background DataConsumer.
 * Incoming data is buffered first.
 * A flush is carried out when the batch size is significant, 
 * a client requesting a flush, or significant amount of time has passed.
 * The data is flushed to the underlying dataloader, which is a DataConsumer.
 * When incoming data comes in too fast, the thread sending data will be put on hold.
 * This acts as incoming data throttling.
 * 
 * @param <R>
 * @param <V>
 */
public class BatchedIndexDataLoader<R extends IndexReader,V> implements DataConsumer<V> {

	protected int _batchSize;
	protected long _delay;
	protected final DataConsumer<ZoieIndexable> _dataLoader;
	protected List<DataEvent<ZoieIndexable>> _batchList;
	protected final LoaderThread _loadMgrThread;
	protected long _lastFlushTime;
	protected int _eventCount;
	protected int _maxBatchSize;
	protected boolean _stop;
	protected boolean _flush;
	protected final SearchIndexManager<R> _idxMgr;
	protected final ZoieIndexableInterpreter<V> _interpreter;
	private final Queue<IndexingEventListener> _lsnrList;
	  
	  private static Logger log = Logger.getLogger(BatchedIndexDataLoader.class);
	  
	  /**
	   * @param dataLoader
	   * @param batchSize
	   * @param maxBatchSize
	   * @param delay
	   * @param idxMgr
	   * @param lsnrList the list of IndexingEventListeners. This should be a <b>Synchronized</b> list if the content of this list is mutable.
	   */
	  public BatchedIndexDataLoader(DataConsumer<ZoieIndexable> dataLoader, int batchSize,int maxBatchSize,long delay,
                                    SearchIndexManager<R> idxMgr,
                                    ZoieIndexableInterpreter<V> interpreter,
                                    Queue<IndexingEventListener> lsnrList)
	  {
	    _maxBatchSize=Math.max(maxBatchSize, 1);
	    _batchSize=Math.min(batchSize, _maxBatchSize);
	    _delay=delay;
	    _dataLoader=dataLoader;
	    _batchList=new LinkedList<DataEvent<ZoieIndexable>>();
	    _lastFlushTime=0L;
	    _eventCount=0;
	    _loadMgrThread=new LoaderThread();
	    _loadMgrThread.setName("disk indexer data loader");
	    _stop=false;
	    _flush=false;
	    _idxMgr = idxMgr;
	    _interpreter = interpreter;
	    _lsnrList = lsnrList;
	  }
	  
	  protected final void fireIndexingEvent(IndexingEvent evt){
		  if (_lsnrList!=null && _lsnrList.size() > 0){
  		    synchronized(_lsnrList) {
  			  for (IndexingEventListener lsnr : _lsnrList){
  				  try{
  				    lsnr.handleIndexingEvent(evt);
  				  }
  				  catch(Exception e){
  					  log.error(e.getMessage(),e);
  				  }
  			  }
  		    }
		  }
	  }
	  
	  public synchronized int getMaxBatchSize()
	  {
	    return _maxBatchSize;
	  }
	  
	  public synchronized void setMaxBatchSize(int maxBatchSize)
	  {
	    _maxBatchSize = Math.max(maxBatchSize, 1);
	    _batchSize = Math.min(_batchSize, _maxBatchSize);
	  }
	  
	  public synchronized int getBatchSize()
	  {
	    return _batchSize;
	  }
	  
	  public synchronized void setBatchSize(int batchSize)
	  {
	    _batchSize=Math.min(Math.max(1, batchSize), _maxBatchSize);
	  }
	  
	  public synchronized long getDelay()
	  {
	    return _delay;
	  }
	  
	  public synchronized void setDelay(long delay)
	  {
	    _delay=delay;
	  }
	  
	  public synchronized int getEventCount()
	  {
	    return _eventCount;
	  }
	  
	  /**
	   * 
	   * @see proj.zoie.api.DataConsumer#consume(java.util.Collection)
	   */
	  public void consume(Collection<DataEvent<V>> events) throws ZoieException
	  {
	    if (events != null)
	    {
	      // PriorityQueue<DataEvent<ZoieIndexable>> indexableList = new
	      // PriorityQueue<DataEvent<ZoieIndexable>>(data.size(), DataEvent.getComparator());
	      ArrayList<DataEvent<ZoieIndexable>> indexableList =
	          new ArrayList<DataEvent<ZoieIndexable>>(events.size());
	      Iterator<DataEvent<V>> iter = events.iterator();
	      while (iter.hasNext())
	      {
	        try
	        {
	          DataEvent<V> event = iter.next();
	          ZoieIndexable indexable = ((ZoieIndexableInterpreter<V>) _interpreter).convertAndInterpret(event.getData());
	          
	          DataEvent<ZoieIndexable> newEvent =
	              new DataEvent<ZoieIndexable>(event.getVersion(), indexable);
	          indexableList.add(newEvent);
	        }
	        catch (Exception e)
	        {
	          log.error(e.getMessage(), e);
	        }
	      }

	      synchronized (this) // this blocks the batch disk loader thread while indexing to RAM
	      {
	        while (_batchList.size() > _maxBatchSize)
	        {
	          // check if load manager thread is alive
	          if(_loadMgrThread == null || !_loadMgrThread.isAlive())
	          {
	            throw new ZoieException("load manager has stopped");
	          }
	          
	          try
	          {
	            this.wait(60000); // 1 min
	          }
	          catch (InterruptedException e)
	          {
	            continue;
	          }
	        }
	        _eventCount += indexableList.size();
	        _batchList.addAll(indexableList);
	        this.notifyAll();
	      }
	    }
	  }
	  
      public synchronized int getCurrentBatchSize()
      {
        return (_batchList != null ? _batchList.size() : 0);
      }
      
      /**
       * This method needs to be called within a synchronized block on 'this'.
       * @return the list of data events already received. A new list is created to receive new data events.
       */
      protected List<DataEvent<ZoieIndexable>> getBatchList()
	  {
        List<DataEvent<ZoieIndexable>> tmpList=_batchList;
        _batchList=new LinkedList<DataEvent<ZoieIndexable>>();
        return tmpList;
	  }
	  
	  /**
	   * Wait for timeOut amount of time for the indexing thread to process data events.
	   * If there are still remaining unprocessed events by the end of timeOut duration,
	   * a ZoieException is thrown.
	   * @param timeOut a timeout value in milliseconds.
	   * @throws ZoieException
	   */
	  public void flushEvents(long timeOut) throws ZoieException
	  {
	    
	    synchronized(this)
	    {
	      while(_eventCount>0)
	      {
	        _flush=true;
	        this.notifyAll();
	        long now1 = System.currentTimeMillis();
		    
	        if (timeOut<=0)
	        {
	          log.error("sync timed out");
	          throw new ZoieException("timed out");          
	        }
	        try
	        {
	          this.wait(timeOut);
	        }
	        catch (InterruptedException e)
	        {
	          throw new ZoieException(e.getMessage());
	        }
	        
	        long now2 = System.currentTimeMillis();
	        
	        timeOut -= (now2 - now1);
	      }
	    }
	  }

	  /**
	   * Used by the indexing thread to flush incoming data events in batch.
	   * A flush is carried out when the batch size is significant, 
	   * a client requesting a flush, or significant amount of time has passed.
	   * The data is flushed to the underlying dataloader, which is a DataConsumer.
	   */
	  protected void processBatch()
	  {
        List<DataEvent<ZoieIndexable>> tmpList=null;
        long now=System.currentTimeMillis();
        long duration=now-_lastFlushTime;

        synchronized(this)
        {
          while(_batchList.size()<_batchSize && !_stop && !_flush && duration<_delay)
          {
            try
            {
              this.wait(_delay - duration);
            }
            catch (InterruptedException e)
            {
              log.warn(e.getMessage());
            }
            now=System.currentTimeMillis();
            duration=now-_lastFlushTime;
          }
          _flush=false;
          _lastFlushTime=now;

          if (_batchList.size()>0)
          {
            // change the status and get the batch list
            // this has to be done in the block synchronized on BatchIndexDataLoader
            _idxMgr.setDiskIndexerStatus(SearchIndexManager.Status.Working);
            tmpList = getBatchList();
          }
        }
        
        if (tmpList != null)
        {
          long t1=System.currentTimeMillis();
          int eventCount = tmpList.size();
          try
          {
            _dataLoader.consume(tmpList);
          }
          catch (ZoieException e)
          {
            log.error(e.getMessage(),e);
          }
          finally
          {
            long t2=System.currentTimeMillis();
            synchronized(this)
            {
              _eventCount -= eventCount;
              log.info(this+" flushed batch of "+eventCount+" events to disk indexer, took: "+(t2-t1)+" current event count: "+_eventCount);
              IndexUpdatedEvent evt = new IndexUpdatedEvent(eventCount,t1,t2,_eventCount);
              fireIndexingEvent(evt);
              this.notifyAll();
            }
          }
        }
        else
        {
          log.debug("batch size is 0");
        }
	  }
	  
	  protected class LoaderThread extends IndexingThread
	  {		  
	    LoaderThread()
	    {
	      super("disk indexer data loader");
	    }
	    
	    public void run()
	    {
	      while(!_stop)
	      {
	        processBatch();
	      }
	    }
	  }
	  
	  /**
	   * Starts the build-in indexing thread.
	   */
	  public void start()
	  {
	    _loadMgrThread.setName(String.valueOf(this));
	    _loadMgrThread.start();
	  }

      /**
       * Shutdown the build-in indexing thread and wait until it dies.
       */
	  public void shutdown()
	  {
	    synchronized(this)
	    {
	      _stop = true;
	      this.notifyAll();
	    }
	    try 
	    {
			_loadMgrThread.join();
		} catch (InterruptedException e) {
			log.error(e.getMessage(),e);
		}
	  }

	  protected static class ZoieIndexableDecorator extends AbstractZoieIndexable{
	    private final ZoieIndexable _inner;
	    private ZoieIndexableDecorator(ZoieIndexable inner){
	      _inner = inner;
	    }

	    public static ZoieIndexableDecorator decorate(ZoieIndexable inner)
	    {
	      return (inner == null ? null : new ZoieIndexableDecorator(inner));
	    }
	    
	    

	    @Override
		public IndexingReq[] buildIndexingReqs() {
			return _inner.buildIndexingReqs();
		}

	    public long getUID() {
	      return _inner.getUID();
	    }

	    public boolean isDeleted() {
	      return _inner.isDeleted();
	    }

	    public boolean isSkip() {
	      return _inner.isSkip();
	    }

	  }
	  
	public long getVersion()
	{
	  throw new UnsupportedOperationException();
	}
}
