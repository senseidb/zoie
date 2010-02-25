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
import proj.zoie.impl.indexing.IndexingEventListener.IndexingEvent;

public final class IndexUpdatedEvent extends IndexingEvent {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final int _numDocsIndexed;
	private final long _startIndexingTime;
	private final long _endIndexingTime;
	private final int _numDocsLeftInQueue;
	
	public IndexUpdatedEvent(int numDocsIndexed,long startIndexingTime,long endIndexingTime,int numDocsLeftInQueue){
		_numDocsIndexed = numDocsIndexed;
		_startIndexingTime = startIndexingTime;
		_endIndexingTime = endIndexingTime;
		_numDocsLeftInQueue = numDocsLeftInQueue;
	}

	public int getNumDocsIndexed() {
		return _numDocsIndexed;
	}

	public long getStartIndexingTime() {
		return _startIndexingTime;
	}

	public long getEndIndexingTime() {
		return _endIndexingTime;
	}

	public int getNumDocsLeftInQueue() {
		return _numDocsLeftInQueue;
	}
}
