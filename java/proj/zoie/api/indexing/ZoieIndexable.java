package proj.zoie.api.indexing;
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
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;

/**
 * Builder object to produce indexing requests.
 */
public interface ZoieIndexable{
	/**
	 * document ID field name
	 * @deprecated this field should no longer be used
	*/
	public static final String DOCUMENT_ID_FIELD = "id";
	  
	/**
	 * Wrapper object for a Lucene {@link org.apache.lucene.document.Document} and an {@link org.apache.lucene.analysis.Analyzer}
	 */
	public static final class IndexingReq{
		private final Document _doc;
		private final Analyzer _analyzer;
		
		public IndexingReq(Document doc){
			this(doc,null);
		}
		
		public IndexingReq(Document doc,Analyzer analyzer){
			_doc = doc;
			_analyzer = analyzer;
		}
		
		public Document getDocument(){
			return _doc;
		}
		
		public Analyzer getAnalyzer(){
			return _analyzer;
		}
	}
	
	/**
	 * Gets the UID.
	 * @return UID
	 */
	long getUID();
	
	/**
	 * Whether this is indexable is delete only. If so, requests returned from {@link #buildIndexingReqs()} will be ignored.
	 * @return true if this is indexable is delete-only.
	 */
	boolean isDeleted();
	
	/**
	 * Whether or not to skip this request. This is useful whether information to decide whether to skip this request is not known
	 * until runtime.
	 * @return true if this is indexable is to be skipped.
	 */
	boolean isSkip();
	
	/**
	 * Builder method.
	 * @return An array of indexing requests
	 */
	IndexingReq[] buildIndexingReqs();
}
