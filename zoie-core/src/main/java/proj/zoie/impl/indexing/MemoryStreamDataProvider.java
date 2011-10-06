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
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieException;

public class MemoryStreamDataProvider<D> extends StreamDataProvider<D>
{

  private List<DataEvent<D>> _list;
  private int _count;
  private volatile String _maxVersion = null;

  // private static final double DEFAULT_ITERS_PER_SECOND=100.0;
  private static final Logger log = Logger.getLogger(MemoryStreamDataProvider.class);

  public MemoryStreamDataProvider(Comparator<String> versionComparator)
  {
    super(versionComparator);
    _list = new LinkedList<DataEvent<D>>();
    _count = 0;
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
      String maxVersion = _maxVersion;
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

  public void addEvents(List<DataEvent<D>> list)
  {
    if (list != null && !list.isEmpty())
    {
      Iterator<DataEvent<D>> iter = list.iterator();
      synchronized (this)
      {
        while (iter.hasNext())
        {
          DataEvent<D> obj = iter.next();
          _maxVersion = _versionComparator.compare(_maxVersion, obj.getVersion())>=0 ? _maxVersion:obj.getVersion();
          _count++;
          _list.add(obj);
        }
        this.notifyAll();
      }
    }
  }

  public void addEvent(DataEvent<D> event)
  {
    if (event != null)
    {
      synchronized (this)
      {
        _maxVersion = _versionComparator.compare(_maxVersion, event.getVersion())>=0 ? _maxVersion:event.getVersion();
        _count++;
        _list.add(event);
        this.notifyAll();
      }
    }
  }

  @Override
  public DataEvent<D> next()
  {
    DataEvent<D> obj = null;
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
  public void setStartingOffset(String version) {
    throw new UnsupportedOperationException("not supported");
  }
}
