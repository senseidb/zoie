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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.mbean.DataProviderAdminMBean;

public abstract class StreamDataProvider<D> implements DataProvider<D>, DataProviderAdminMBean
{
  private static final Logger log = Logger.getLogger(StreamDataProvider.class);

  private int _batchSize;
  private DataConsumer<D> _consumer;
  private DataThread<D> _thread;
  private volatile int _retryTime = 100;   // default retry every 100ms

  protected final Comparator<String> _versionComparator;

  public StreamDataProvider(Comparator<String> versionComparator)
  {
    _batchSize = 1;
    _consumer = null;
    _versionComparator = versionComparator;
  }
  
  public void setRetryTime(int retryTime){
	_retryTime = retryTime;
  }
  
  public int getRetryTime(){
	return _retryTime;
  }

  public void setDataConsumer(DataConsumer<D> consumer)
  {
    _consumer = consumer;
  }

  public DataConsumer<D> getDataConsumer()
  {
    return _consumer;
  }

  public abstract DataEvent<D> next();

  public abstract void setStartingOffset(String version);
  
  public abstract void reset();

  public int getBatchSize()
  {
    return _batchSize;
  }

  public long getEventsPerMinute()
  {
    DataThread<D> thread = _thread;
    if (thread == null)
      return 0;
    return thread.getEventsPerMinute();
  }

  public long getMaxEventsPerMinute()
  {
    return _maxEventsPerMinute;
  }

  private volatile long _maxEventsPerMinute = Long.MAX_VALUE;// begin with no
  private volatile long _maxVolatileTimeInMillis = Long.MAX_VALUE; // begin with no volatile time limit  

  // indexing

  public void setMaxEventsPerMinute(long maxEventsPerMinute)
  {
    _maxEventsPerMinute = maxEventsPerMinute;
    DataThread<D> thread = _thread;
    if (thread == null)
      return;
    thread.setMaxEventsPerMinute(_maxEventsPerMinute);
  }
  
  public void setMaxVolatileTime(long timeInMillis) {
    _maxVolatileTimeInMillis = timeInMillis;
    DataThread<D> thread = _thread;
    if (thread == null) return;
    thread.setMaxVolatileTime(_maxVolatileTimeInMillis);
  }

  public String getStatus()
  {
    DataThread<D> thread = _thread;
    if (thread == null)
      return "dead";
    return thread.getStatus() + " : " + thread.getState();
  }

  public void pause()
  {
    if (_thread != null)
    {
      _thread.pauseDataFeed();
    }
  }

  public void resume()
  {
    if (_thread != null)
    {
      _thread.resumeDataFeed();
    }
  }

  public void setBatchSize(int batchSize)
  {
    _batchSize = Math.max(1, batchSize);
  }

  public long getEventCount()
  {
    DataThread<D> thread = _thread;
    if (thread != null)
      return _thread.getEventCount();
    else
      return 0;
  }

  public void stop()
  {
    if (_thread != null && _thread.isAlive())
    {
      _thread.terminate();
      try
      {
        _thread.join();
      } catch (InterruptedException e)
      {
        log.warn("stopping interrupted");
      }
    }
  }

  public void start()
  {
    if (_thread == null || !_thread.isAlive())
    {
      reset();

      _thread = new DataThread<D>(this);
      _thread.setMaxEventsPerMinute(_maxEventsPerMinute);
      _thread.setMaxVolatileTime(_maxVolatileTimeInMillis);
      _thread.start();
    }
  }

  public void syncWithVersion(long timeInMillis, String version) throws ZoieException
  {
    _thread.syncWithVersion(timeInMillis, version);
  }

  private static final class DataThread<D> extends Thread
  {
    private Collection<DataEvent<D>> _batch;
    private volatile String _currentVersion;
    private final StreamDataProvider<D> _dataProvider;
    private volatile boolean _paused;
    private volatile boolean _stop;
    private AtomicLong _eventCount = new AtomicLong(0);
    private volatile long _throttle = 40000;
    private volatile long _maxVolatileTimeInMillis = Long.MAX_VALUE;
    private volatile long _lastFlushTime = System.currentTimeMillis();
    private boolean _flushing = false;
    private final Comparator<String> _versionComparator;

    private void resetEventTimer()
    {
      _eventCount.set(0);
    }

    private String getStatus()
    {
      synchronized (this)
      {
        if (_stop)
          return "stopped";
        if (_paused)
          return "paused";
        return "running";
      }
    }

    DataThread(StreamDataProvider<D> dataProvider)
    {
      super("Stream DataThread");
      setDaemon(false);
      _dataProvider = dataProvider;
      _currentVersion = null;
      _paused = false;
      _stop = false;
      _batch = new LinkedList<DataEvent<D>>();
      _versionComparator = dataProvider._versionComparator;
    }

    @Override
    public void start()
    {
      super.start();
      resetEventTimer();
    }

    void terminate()
    {
      _stop = true;
      synchronized (this){
        this.notifyAll();
      }
    }

    void pauseDataFeed()
    {
        _paused = true;
    }

    void resumeDataFeed()
    {
      synchronized (this)
      {
        _paused = false;
        resetEventTimer();
        this.notifyAll();
      }
    }

    private void flush()
    {
      // FLUSH
      Collection<DataEvent<D>> tmp;
      tmp = _batch;
      _batch = new LinkedList<DataEvent<D>>();

      try
      {
        if (_dataProvider._consumer != null)
        {
          int batchSize = tmp.size();
          _dataProvider._consumer.consume(tmp);
          _eventCount.getAndAdd(batchSize);
          updateStats();
        }
      } catch (ZoieException e)
      {
        log.error(e.getMessage(), e);
      }
      _lastFlushTime = System.currentTimeMillis();
    }

    private long lastcount = 0;

    private synchronized void updateStats()
    {
      long newcount = _eventCount.get();
      long count = newcount - lastcount;
      lastcount = newcount;
      long now = System.nanoTime();
      if (now - last60slots[currentslot] > 1000000000L)
      {
        // in nano seconds, passed one second
        currentslot = (currentslot + 1) % last60.length;
        last60slots[currentslot] = now;
        last60[currentslot] = 0;
      }
      last60[currentslot] += count;
    }

    public void syncWithVersion(long timeInMillis, String version) throws ZoieException
    {
      if (version == null) return;
      long now = System.currentTimeMillis();
      long due = now + timeInMillis;
      synchronized (this)
      {
        try
        {
          while (_currentVersion == null || _versionComparator.compare(_currentVersion, version) < 0)
          {
            if (now >= due)
            {
              throw new ZoieException("sync timed out");
            }
            try
            {
              this.notifyAll();
              _flushing = true;
              this.wait(Math.min(due - now, 200));
            } catch (InterruptedException e)
            {
              log.warn(e.getMessage(), e);
            }
            now = System.currentTimeMillis();
          }
        } finally
        {
          _flushing = false;
        }
      }
    }

    public void run()
    {
      String version = _currentVersion;
      while (!_stop)
      {
        updateStats();
        synchronized (this)
        {
          while (!_stop && (_paused || (getEventsPerMinute() > _throttle)))
          {
            try
            {
              this.wait(500);
            } catch (InterruptedException e)
            {
              Thread.interrupted();
              continue;
            }
            updateStats();
          }
        }
        if (!_stop)
        {
          DataEvent<D> data = _dataProvider.next();
          if (data != null)
          {
            version = _versionComparator.compare(version, data.getVersion())>=0 ? version:data.getVersion();
            synchronized (this)
            {
              _batch.add(data);
              if (_batch.size() >= _dataProvider._batchSize || _flushing
                  || System.currentTimeMillis() - _lastFlushTime > _maxVolatileTimeInMillis)
              {
                flush();
                _currentVersion = version;
                this.notifyAll();
              }
            }
          } else
          {
            synchronized (this)
            {
              if (_flushing && (_batch.size() > 0))
              {
                flush();
                _currentVersion = version;
              }
              this.notifyAll();
              try
              {
                this.wait(_dataProvider.getRetryTime());
              } catch (InterruptedException e)
              {
                Thread.interrupted();
              }
            }
          }
        }
      }
      flush();
    }

    private long getEventCount()
    {
      return _eventCount.get();
    }

    private long[] last60 = new long[60];
    private long[] last60slots = new long[60];
    private volatile int currentslot = 0;
    private static final int window = 3;// window size 3 seconds

    private long getEventsPerMinute()
    {
      int slot = currentslot;
      long countwindow = 0;
      long count = 0;
      for (int i = 0; i < 60; i++)
      {
        int id = (slot - i + 60) % 60;
        if (i < window)
          countwindow += last60[id];
        count += last60[id];
      }
      // use the higher of the rates in the time window and last 60 seconds
      return Math.max(countwindow * 60 / window, count);
    }

    private void setMaxEventsPerMinute(long maxEventsPerMinute)
    {
      _throttle = maxEventsPerMinute;
    }
    
    private void setMaxVolatileTime(long timeInMillis)
    {
      _maxVolatileTimeInMillis = timeInMillis;
    }

  }
}
