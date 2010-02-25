
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Similarity;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.IndexUpdatedEvent;
import proj.zoie.impl.indexing.IndexingEventListener;

/**
 * Keeps track of the number of incoming data events.
 * 
 * @author ymatsuda, xgu
 *
 */
public class RealtimeIndexDataLoader<R extends IndexReader, V> extends BatchedIndexDataLoader<R,V>
{
  private int _currentBatchSize;
  private final DataConsumer<ZoieIndexable>  _ramConsumer;
  private final DiskLuceneIndexDataLoader<R> _luceneDataLoader;
  private final Analyzer                     _analyzer;
  private final Similarity                   _similarity;
  
  private static Logger log = Logger.getLogger(RealtimeIndexDataLoader.class);
  
  public RealtimeIndexDataLoader(DiskLuceneIndexDataLoader<R> dataLoader, int batchSize,int maxBatchSize,long delay,
                                 Analyzer analyzer,
                                 Similarity similarity,
                                 SearchIndexManager<R> idxMgr,
                                 ZoieIndexableInterpreter<V> interpreter,
                                 Queue<IndexingEventListener> lsnrList)
  {
    super(dataLoader, batchSize, maxBatchSize, delay, idxMgr, interpreter, lsnrList);
    _analyzer = analyzer;
    _similarity = similarity;
    _currentBatchSize = 0;
    _ramConsumer = new RAMLuceneIndexDataLoader<R>(_analyzer, _similarity, _idxMgr);
    _luceneDataLoader = dataLoader;
  }
  
  /* (non-Javadoc)
   * @see proj.zoie.impl.indexing.internal.BatchedIndexDataLoader#consume(java.util.Collection)
   */
  @Override
  public void consume(Collection<DataEvent<V>> events) throws ZoieException
  {
    if (events != null)
    {
      ArrayList<DataEvent<ZoieIndexable>> indexableList =
          new ArrayList<DataEvent<ZoieIndexable>>(events.size());
      Iterator<DataEvent<V>> iter = events.iterator();
      while (iter.hasNext())
      {
        try
        {
          DataEvent<V> event = iter.next();
          ZoieIndexable indexable =
                ((ZoieIndexableInterpreter<V>) _interpreter).convertAndInterpret(event.getData());
          
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
        int size = indexableList.size();
        _ramConsumer.consume(indexableList);// consumer clear the list!
        _currentBatchSize += size;
        _eventCount += size;
        
        while (_currentBatchSize > _maxBatchSize)
        {
          // check if load manager thread is alive
          if(_loadMgrThread == null || !_loadMgrThread.isAlive())
          {
            throw new ZoieException("load manager has stopped");
          }
          
          this.notifyAll(); // wake up load manager thread      
          
          try
          {
            this.wait(60000); // 1 min
          }
          catch (InterruptedException e)
          {
            continue;
          }
        }
      }
    }
  }
  
  public synchronized int getCurrentBatchSize()
  {
    return _currentBatchSize;
  }
  
  @Override
  protected synchronized void processBatch()
  {
    RAMSearchIndex<R> readOnlyMemIndex = null;
    long now = System.currentTimeMillis();
    long duration = now - _lastFlushTime;
    int eventCount = 0;
    while(_currentBatchSize < _batchSize && !_stop && !_flush && duration < _delay)
    {
      try
      {
        wait(_delay - duration);
      }
      catch (InterruptedException e)
      {
        log.warn(e.getMessage());
      }
      now = System.currentTimeMillis();
      duration = now - _lastFlushTime;
    }
    _flush = false;
    _lastFlushTime = now;

    if (_currentBatchSize > 0)
    {
      // change the status and get the read only memory index
      // this has to be done in the block synchronized on CopyingBatchIndexDataLoader
      _idxMgr.setDiskIndexerStatus(SearchIndexManager.Status.Working);
      readOnlyMemIndex = _idxMgr.getCurrentReadOnlyMemoryIndex();
      eventCount = _currentBatchSize;
      _currentBatchSize = 0;
    }

    if (eventCount > 0)
    {
      long t1=System.currentTimeMillis();
      try
      {
        if(readOnlyMemIndex != null)
          _luceneDataLoader.loadFromIndex(readOnlyMemIndex);
      }
      catch (ZoieException e)
      {
        log.error(e.getMessage(),e);
      }
      finally
      {
        long t2=System.currentTimeMillis();
        _eventCount -= eventCount;
        log.info(this+" flushed batch of "+eventCount+" events to disk indexer, took: "+(t2-t1)+" current event count: "+_eventCount);
        IndexUpdatedEvent evt = new IndexUpdatedEvent(eventCount,t1,t2,_eventCount);
        fireIndexingEvent(evt);
        notifyAll();
      }
    }
    else
    {
      log.debug("batch size is 0");
    }
  }
}
