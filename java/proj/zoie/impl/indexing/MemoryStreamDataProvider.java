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

public class MemoryStreamDataProvider<V> extends StreamDataProvider<V> {

	  private List<DataEvent<V>> _list;
	  private int _count;
	  private boolean _stop;
	  
	  // private static final double DEFAULT_ITERS_PER_SECOND=100.0;
	  private static final Logger log = Logger.getLogger(MemoryStreamDataProvider.class);
	  
	  public MemoryStreamDataProvider()
	  {
	    super();
	    _list= new LinkedList<DataEvent<V>>();
	    _count=0;
	    _stop=false;
	  }
	  
	  @Override
	  public void reset()
	  {
	    synchronized(this)
	    {
	      _list.clear();
	      this.notifyAll();
	    }
	  }
	  
	  public void flush()
	  {
        synchronized(this)
        {
          while(!_list.isEmpty() && !_stop)
          {
            this.notifyAll();			
            try
            {
              this.wait();
            }
            catch(InterruptedException e)
            {
              log.warn(e.getMessage());
            }
          }    	    	
	    } 
	  }
	  
      public void addEvents(List<DataEvent<V>> list)
      {
        if (list!=null && !list.isEmpty())
        {
          Iterator<DataEvent<V>> iter=list.iterator();
          synchronized(this)
          {
            while(iter.hasNext())
            {
              DataEvent<V> obj=iter.next();
              _count++;
              _list.add(obj);
            }
            this.notifyAll();
          }
        }
      }

      public void addEvent(DataEvent<V> event)
      {
        if (event!=null)
        {
          synchronized(this)
          {
            _count++;
            _list.add(event);
            this.notifyAll();
          }
        }
      }
	  
	  @Override
	  public DataEvent<V> next()
	  {
		DataEvent<V> obj=null;
        synchronized(this)
        {
          while(_list.isEmpty() && !_stop)
          {
            try 
            {
              this.wait();
            }
            catch (InterruptedException e) 
            {
              log.warn(e.getMessage());
            }
          }
          if (!_list.isEmpty())
          {
            obj=_list.remove(0);
            this.notifyAll();
          }
        }
	    return obj;
	  }
	  
	  public int getCount()
	  {
	    synchronized(this)
	    {
	      return _count;
        }
	  }

	  @Override
	  public void stop()
	  {
	    try
	    {
	      synchronized(this)
	      {
	        _stop=true;
	        this.notifyAll();
	      }
	    }
	    finally
	    {
	      super.stop();
	    }
	  }
}
