package proj.zoie.api;
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

/**
 * interface for consuming a collection of data events
 */
public interface DataConsumer<V> {
	
	/**
	 * Data event abstraction.
	 */
	public static final class DataEvent<V>
	{
		static Comparator<DataEvent<?>> VERSION_COMPARATOR = new EventVersionComparator();
		private long _version;
		private V _data;
		
		/**
		 * Create a data event instance
		 * @param version Version of the event
		 * @param data Data for the event
		 */
		public DataEvent(long version,V data)
		{
			_data=data;
			_version=version;
		}
		
		/**
		 * Gets the version.
		 * @return Version of the vent
		 */
		public long getVersion()
		{
			return _version;
		}
		
		/**
		 * Gets the data.
		 * @return Data for the event.
		 */
		public V getData()
		{
			return _data;
		}
	    
		static public Comparator<DataEvent<?>> getComparator()
		{
		  return VERSION_COMPARATOR;
		}
		
	    static public class EventVersionComparator implements Comparator<DataEvent<?>>
	    {
	      public int compare(DataEvent<?> o1, DataEvent<?> o2)
          {
            if(o1._version < o2._version) return -1;
            else if(o1._version > o2._version) return 1;
            else return 0; 
          }
	      public boolean equals(DataEvent<?> o1, DataEvent<?> o2)
	      {
	        return (o1._version == o2._version);
	      }
	    }
	}
	
	/**
	 * Consumption of a collection of data events.
	 * Note that this method may have a side effect. That is it may empty the Collection passed in after execution.
	 * It is good practice if the data collection along with its contents passed to consume(data) never changed by client code later.
	 * We also strengthen the contract on this method that the events in data are sorted according to their version numbers and
	 * if consume is invoked on collection1 before on collection2, then all the max version number for events in collection1
	 * must be smaller than the min version number for events in collection2.
	 * @param data A collection of data to be consumed.
	 * @throws ZoieException
	 */
	void consume(Collection<DataEvent<V>> data) throws ZoieException;

	/**
	 * This method is not meant to be Thread-Safe, since that could add significant
	 * complexity and performance issue. When Thread-Safety is desired, the client
	 * should add external synchronization.
   * @return the version number of events that it has received but not necessarily processed.
   */
	long getVersion();
}
