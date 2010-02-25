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

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class DelegateIndexDataConsumer<V> implements DataConsumer<V> {
	private static final Logger log = Logger.getLogger(DelegateIndexDataConsumer.class);
	private final DataConsumer<ZoieIndexable> _diskConsumer;
	private final DataConsumer<ZoieIndexable> _ramConsumer;
	private final ZoieIndexableInterpreter<V> _interpreter;
	
	public DelegateIndexDataConsumer(DataConsumer<ZoieIndexable> diskConsumer,DataConsumer<ZoieIndexable> ramConsumer,ZoieIndexableInterpreter<V> interpreter)
	{
	  	_diskConsumer=diskConsumer;
	  	_ramConsumer=ramConsumer;
	  	_interpreter=interpreter;
	}
	
	public void consume(Collection<DataEvent<V>> data)
			throws ZoieException {
		if (data!=null)
		{
		  //PriorityQueue<DataEvent<ZoieIndexable>> indexableList = new PriorityQueue<DataEvent<ZoieIndexable>>(data.size(), DataEvent.getComparator());
		  ArrayList<DataEvent<ZoieIndexable>> indexableList=new ArrayList<DataEvent<ZoieIndexable>>(data.size());
		  Iterator<DataEvent<V>> iter=data.iterator();
		  while(iter.hasNext())
		  {
			  try{
			    DataEvent<V> event=iter.next();
			    ZoieIndexable indexable = ((ZoieIndexableInterpreter<V>)_interpreter).convertAndInterpret(event.getData());
			   
			    DataEvent<ZoieIndexable> newEvent=new DataEvent<ZoieIndexable>(event.getVersion(),indexable);
			    indexableList.add(newEvent);
			  }
			  catch(Exception e){
				log.error(e.getMessage(),e);
			  }
		  }
		  
		  if(_diskConsumer != null)
		  {
		    synchronized(_diskConsumer) // this blocks the batch disk loader thread while indexing to RAM
		    {
	          if (_ramConsumer != null)
	          {
	            ArrayList<DataEvent<ZoieIndexable>> ramList=new ArrayList<DataEvent<ZoieIndexable>>(indexableList);
	            _ramConsumer.consume(ramList);
	          }
	          _diskConsumer.consume(indexableList);
		    }
		  }
		  else
		  {
		    if (_ramConsumer != null)
		    {
			  _ramConsumer.consume(indexableList);
		    }
		  }
		}
	}
  
  public long getVersion()
  {
    throw new UnsupportedOperationException();
  }
}
