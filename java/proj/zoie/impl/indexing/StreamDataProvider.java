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
import proj.zoie.api.ZoieVersion;
import proj.zoie.api.DataProvider;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.mbean.DataProviderAdminMBean;

public abstract class StreamDataProvider<D, V extends ZoieVersion> implements DataProvider<D>, DataProviderAdminMBean
{
  private static final Logger log = Logger.getLogger(StreamDataProvider.class);

  private int _batchSize;
  private DataConsumer<D, V> _consumer;
  private DataThread<D, V> _thread;

  public StreamDataProvider()
  {
    _batchSize = 1;
    _consumer = null;
  }

  public void setDataConsumer(DataConsumer<D, V> consumer)
  {
    _consumer = consumer;
  }

  public DataConsumer<D, V> getDataConsumer()
  {
    return _consumer;
  }

  public abstract DataEvent<D, V> next();

  public abstract void reset();

  public int getBatchSize()
  {
    return _batchSize;
  }

  public long getEventsPerMinute()
  {
    DataThread<D, V> thread = _thread;
    if (thread == null)
      return 0;
    return thread.getEventsPerMinute();
  }

  public long getMaxEventsPerMinute()
  {
    return _maxEventsPerMinute;
  }

  private volatile long _maxEventsPerMinute = Long.MAX_VALUE;// begin with no

  // indexing

  public void setMaxEventsPerMinute(long maxEventsPerMinute)
  {
    _maxEventsPerMinute = maxEventsPerMinute;
    DataThread<D, V> thread = _thread;
    if (thread == null)
      return;
    thread.setMaxEventsPerMinute(_maxEventsPerMinute);

  }

  public String getStatus()
  {
    DataThread<D, V> thread = _thread;
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
    DataThread<D, V> thread = _thread;
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

      _thread = new DataThread<D, V>(this);
      _thread.setMaxEventsPerMinute(_maxEventsPerMinute);

      _thread.start();
    }
  }

  public void syncWithVersion(long timeToWait, V version) throws ZoieException
  {
    _thread.syncWthVersion(timeToWait, version);
  }

  public void syncWthVersion(long timeInMillis, V version) throws ZoieException
  {
    _thread.syncWthVersion(timeInMillis, version);
  }

  private static final class DataThread<D, V extends ZoieVersion> extends Thread
  {
    private Collection<DataEvent<D, V>> _batch;
    private V _currentVersion;
    private final StreamDataProvider<D, V> _dataProvider;
    private boolean _paused;
    private boolean _stop;
    private volatile boolean _stopped = false;
    private AtomicLong _eventCount = new AtomicLong(0);
    private volatile long _throttle = 40000;// Long.MAX_VALUE;
    private boolean _flushing = false;

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

    DataThread(StreamDataProvider<D, V> dataProvider)
    {
      super("Stream DataThread");
      setDaemon(false);
      _dataProvider = dataProvider;
      _currentVersion = null;
      _paused = false;
      _stop = false;
      _batch = new LinkedList<DataEvent<D, V>>();
    }

    @Override
    public void start()
    {
      super.start();
      resetEventTimer();
    }

    void terminate()
    {
      synchronized (this)
      {
        _stop = true;
        while (!_stopped)
        {
          this.notifyAll();
          try
          {
            this.wait(100);
          } catch (InterruptedException e)
          {
            Thread.interrupted();
          }
        }
      }
    }

    void pauseDataFeed()
    {
      synchronized (this)
      {
        _paused = true;
      }
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
      Collection<DataEvent<D, V>> tmp;
      tmp = _batch;
      _batch = new LinkedList<DataEvent<D, V>>();

      try
      {
        if (_dataProvider._consumer != null)
        {
          _eventCount.getAndAdd(tmp.size());
          updateStats();
          _dataProvider._consumer.consume(tmp);
        }
      } catch (ZoieException e)
      {
        log.error(e.getMessage(), e);
      }
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

    public V getCurrentVersion()
    {
      synchronized (this)
      {
        return _currentVersion;
      }
    }

    public void syncWthVersion(long timeInMillis, V version) throws ZoieException
    {
      long now = System.currentTimeMillis();
      long due = now + timeInMillis;
      synchronized (this)
      {
        try
        {
          while (_currentVersion == null || _currentVersion.compareTo(version) < 0)
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
      V version = _currentVersion;
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
          DataEvent<D, V> data = _dataProvider.next();
          if (data != null)
          {
            version = ZoieVersion.max(version, data.getVersion());
            synchronized (this)
            {
              _batch.add(data);
              if (_batch.size() >= _dataProvider._batchSize || _flushing)
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
                this.wait(100);
              } catch (InterruptedException e)
              {
                Thread.interrupted();
              }
            }
          }
        }
      }
      _stopped = true;
    }

    private long getEventCount()
    {
      return _eventCount.get();
    }

    private long[] last60 = new long[60];
    private long[] last60slots = new long[60];
    private volatile int currentslot = 0;
    private final int window = 3;// window size 3 seconds

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

  }
}
