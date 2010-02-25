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
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.mbean.DataProviderAdminMBean;

public abstract class StreamDataProvider<V> implements DataProvider<V>,DataProviderAdminMBean{
	private static final Logger log = Logger.getLogger(StreamDataProvider.class);
	
	private int _batchSize;
	private DataConsumer<V> _consumer;
	private DataThread<V> _thread;
	
	public StreamDataProvider()
	{
		_batchSize=1;
		_consumer=null;
	}
	
	public void setDataConsumer(DataConsumer<V> consumer)
	{
	  _consumer=consumer;	
	}
	
	public DataConsumer<V> getDataConsumer()
	{
	  return _consumer;
	}

	public abstract DataEvent<V> next();
	
	public abstract void reset();
	
	public int getBatchSize() {
		return _batchSize;
	}
	
	public long getEventsPerMinute() {
	  DataThread<V> thread = _thread;
	  if (thread==null) return 0;
	  return thread.getEventsPerMinute();
	}
	
	public long getMaxEventsPerMinute() {
      DataThread<V> thread = _thread;
      if (thread==null) return 0;
      return thread.getMaxEventsPerMinute();
	}

	public void setMaxEventsPerMinute(long maxEventsPerMinute) {
      DataThread<V> thread = _thread;
      if (thread==null) return;
      thread.setMaxEventsPerMinute(maxEventsPerMinute);
	}
	
	public String getStatus() {
      DataThread<V> thread = _thread;
      if (thread==null) return "dead";
      return thread.getStatus() + " : " + thread.getState();
	}

	public void pause() {
		if (_thread != null)
		{
			_thread.pauseDataFeed();
		}
	}

	public void resume() {
		if (_thread != null)
		{
			_thread.resumeDataFeed();
		}
	}

	public void setBatchSize(int batchSize) {
		_batchSize=Math.max(1, batchSize);
	}
	
	public long getEventCount()
	{
	  DataThread<V> thread = _thread;
	  if (thread != null)
	    return _thread.getEventCount();
	  else
	    return 0;
	}
	
	public void stop()
	{
		if (_thread!=null && _thread.isAlive())
		{
			_thread.terminate();
			try {
				_thread.join();
			} catch (InterruptedException e) {
				log.warn("stopping interrupted");
			}
		}
	}

	public void start() {
		if (_thread==null || !_thread.isAlive())
		{
			reset();
			_thread = new DataThread<V>(this);
			_thread.start();
		}
	}
	
	public void syncWthVersion(long timeInMillis, long version) throws ZoieException
	{
	  _thread.syncWthVersion(timeInMillis, version);
	}
	
	private static final class DataThread<V> extends Thread
	{
	    private Collection<DataEvent<V>> _batch;
		private long _currentVersion;
		private final StreamDataProvider<V> _dataProvider;
		private boolean _paused;
		private boolean _stop;
		private AtomicLong _eventCount = new AtomicLong(0);
		private volatile long _eventStart = System.nanoTime();
		private volatile long _throttle = 40000;//Long.MAX_VALUE;
		
		private void resetEventTimer()
		{
		  _eventCount.set(0);
		  _eventStart = System.nanoTime();
		}
		
		private String getStatus()
		{
		  synchronized(this)
		  {
		    if (_stop) return "stopped";
		    if (_paused) return "paused";
		    return "running";
		  }
		}
		
    DataThread(StreamDataProvider<V> dataProvider)
		{
			super("Stream DataThread");
			setDaemon(false);
			_dataProvider = dataProvider;
			_currentVersion = 0L;
			_paused = false;
			_stop = false;
			_batch = new LinkedList<DataEvent<V>>();
		}
		@Override
		public void start()
		{
		  super.start();
		  resetEventTimer();
		}
		
		void terminate()
		{
			synchronized(this)
			{
	            _stop = true;
			   this.notifyAll();
			}
		}
		
		void pauseDataFeed()
		{
		    synchronized(this)
		    {
		        _paused = true;
		    }
		}
		
		void resumeDataFeed()
		{
			synchronized(this)
			{
	            _paused = false;
	            resetEventTimer();
				this.notifyAll();
			}
		}
		
		private void flush()
	    {
	    	// FLUSH
		    Collection<DataEvent<V>> tmp;
		    tmp = _batch;
            _batch = new LinkedList<DataEvent<V>>();

		    try
	        {
		      if(_dataProvider._consumer!=null)
		      {
		        _eventCount.getAndAdd(tmp.size());
		    	  _dataProvider._consumer.consume(tmp);
		      }
	        }
	        catch (ZoieException e)
	        {
	          log.error(e.getMessage(), e);
	        }
	    }
		
		public long getCurrentVersion()
		{
			synchronized(this)
			{
		      return _currentVersion;
			}
		}
		
		public void syncWthVersion(long timeInMillis, long version) throws ZoieException
		{
		  long now = System.currentTimeMillis();
		  long due = now + timeInMillis;
		  synchronized(this)
		  {
		    while(_currentVersion < version)
		    {
		      if(now > due)
              {
                throw new ZoieException("sync timed out");
              }
		      try
		      {
		        this.wait(due - now);
		      }
		      catch(InterruptedException e)
		      {
	              log.warn(e.getMessage(), e);
		      }
		      now = System.currentTimeMillis();
		    }
		  }
		}

		public void run()
		{
		  while (!_stop)
		  {
		    synchronized(this)
		    {
		      while(!_stop && (_paused || (getEventsPerMinute() > _throttle)))
		      {
		        try {
		          this.wait(500);
		        } catch (InterruptedException e) {
		          continue;
		        }
		      }
		    }
		    if (!_stop)
		    {
		      DataEvent<V> data = _dataProvider.next();
		      if (data!=null)
		      {
		        synchronized(this)
		        {
		          _batch.add(data);
		          if (_batch.size()>=_dataProvider._batchSize)
		          {
		            flush();
		          }
		          _currentVersion=Math.max(_currentVersion, data.getVersion());
		          this.notifyAll();
		        }
		      }
		      else
		      {
		        synchronized(this)
		        {
		          flush();
		          _stop=true;
		          return;
		        }
		      }
		    }
		  }
		}

		private long getEventCount()
		{
		  return _eventCount.get();
		}

		private long getEventsPerMinute()
		{
		  if (! ( (System.nanoTime() - _eventStart)/1000000>0) ) return 0;
		  return _eventCount.get()*60000/((System.nanoTime() - _eventStart)/1000000);
		}
		
		private long getMaxEventsPerMinute()
		{
		  return _throttle;
		}
		
        private void setMaxEventsPerMinute(long maxEventsPerMinute)
        {
          _throttle = maxEventsPerMinute;
        }
	}
}
