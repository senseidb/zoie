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
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

/**
 * This interface is IndexReader instances are to be managed.
 */
public interface IndexReaderFactory<R extends IndexReader> {
	
	/**
	 * Returns a list of index readers.
	 * @return List of IndexReader instances 
	 * @throws IOException
	 */
	List<R> getIndexReaders() throws IOException;
	
	/**
	 * Gets the default analyzer used for indexing.
	 * @return Default analyzer
	 */
	Analyzer getAnalyzer();
	
	/**
	 * Returns the index readers.
	 * @param r Returns the list of index reader instances.
	 */
	void returnIndexReaders(List<R> r);
}
