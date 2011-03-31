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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieVersion;

public class MemoryStreamDataProvider<D, V extends ZoieVersion> extends StreamDataProvider<D, V>
{

  private List<DataEvent<D, V>> _list;
  private int _count;
  private volatile V _maxVersion = null;
  private boolean _stop;

  // private static final double DEFAULT_ITERS_PER_SECOND=100.0;
  private static final Logger log = Logger.getLogger(MemoryStreamDataProvider.class);

  public MemoryStreamDataProvider()
  {
    super();
    _list = new LinkedList<DataEvent<D, V>>();
    _count = 0;
    _stop = false;
  }
  

  @Override
  public void reset()
  {
    synchronized (this)
    {
      _list.clear();
      this.notifyAll();
    }
  }

  /**
   * flush to the max version that has been added. We only guarantee whatever
   * was already added. If more events are added after the beginning of this
   * call, they may or may not be flushed at the return of this call. This
   * method is not supposed to be called too often.
   */
  public void flush()
  {
    try
    {
      V maxVersion = _maxVersion;
      if (log.isDebugEnabled()){
        log.debug("flushing version: " + maxVersion);
      }
      super.syncWithVersion(3600000, maxVersion);
      if (log.isDebugEnabled()){
        log.info("flushing version: " + maxVersion + " done");
      }
    } catch (ZoieException e)
    {
      log.error("flush timeout", e);
    }
  }

  public void addEvents(List<DataEvent<D, V>> list)
  {
    if (list != null && !list.isEmpty())
    {
      Iterator<DataEvent<D, V>> iter = list.iterator();
      synchronized (this)
      {
        while (iter.hasNext())
        {
          DataEvent<D, V> obj = iter.next();
          _maxVersion = ZoieVersion.max(_maxVersion, obj.getVersion());
          _count++;
          _list.add(obj);
        }
        this.notifyAll();
      }
    }
  }

  public void addEvent(DataEvent<D, V> event)
  {
    if (event != null)
    {
      synchronized (this)
      {
        _maxVersion = ZoieVersion.max(_maxVersion, event.getVersion());
        _count++;
        _list.add(event);
        this.notifyAll();
      }
    }
  }

  @Override
  public DataEvent<D, V> next()
  {
    DataEvent<D, V> obj = null;
    synchronized (this)
    {
      if (!_list.isEmpty())
      {
        obj = _list.remove(0);
        this.notifyAll();
      }
    }
    return obj;
  }

  public int getCount()
  {
    synchronized (this)
    {
      return _count;
    }
  }

  @Override
  public void stop()
  {
    try
    {
      synchronized (this)
      {
        _stop = true;
        this.notifyAll();
      }
    } finally
    {
      super.stop();
    }
  }


  @Override
  public void setStartingOffset(V version) {
    throw new UnsupportedOperationException("not supported");
  }
}
