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

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.RAMDirectory;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class RAMSearchIndex<R extends IndexReader> extends BaseSearchIndex<R> {
	  private long         _version;
	  private final RAMDirectory _directory;
	  private final IndexReaderDecorator<R> _decorator;
	  
	  // a consistent pair of reader and deleted set
      private volatile ZoieIndexReader<R> _currentReader;
	  
	  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

	  RAMSearchIndex(long version, IndexReaderDecorator<R> decorator,SearchIndexManager<R> idxMgr){
		super(idxMgr);
	    _directory = new RAMDirectory();
	    _version = version;
	    _decorator = decorator;
	    _currentReader = null;
//	    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler();
//	    cms.setMaxThreadCount(1);
	    _mergeScheduler = new SerialMergeScheduler();
	  }
	  
	  public void close()
	  {
	    super.close();
	    if (_directory!=null)
	    {
	      _directory.close();
	    }
	  }
	  
	  public long getVersion()
	  {
	    return _version;
	  }

	  public void setVersion(long version)
	      throws IOException
	  {
	    _version = version;
	  }

	  public int getNumdocs()
	  {
		ZoieIndexReader<R> reader=null;
	    try
	    {
	      reader=openIndexReader();
	    }
	    catch (IOException e)
	    {
	      log.error(e.getMessage(),e);
	    }
	    
	    if (reader!=null)
	    {
	      return reader.numDocs();
	    }
	    else
	    {
	      return 0;
	    }
	  }
	  
	  @Override
	  public ZoieIndexReader<R> openIndexReader() throws IOException
	  {
	    return _currentReader;
	  }
      
	  @Override
	  protected IndexReader openIndexReaderForDelete() throws IOException {
		if (IndexReader.indexExists(_directory)){
		  return IndexReader.open(_directory,false);
		}
		else{
			return null;
		}
	  }
	  
      private ZoieIndexReader<R> openIndexReaderInternal() throws IOException
      {
	    if (IndexReader.indexExists(_directory))
	    {
	      IndexReader srcReader=null;
	      ZoieIndexReader<R> finalReader=null;
	      try
	      {
	        // for RAM indexes, just get a new index reader
	    	srcReader=IndexReader.open(_directory,true);
	    	finalReader=ZoieIndexReader.open(srcReader, _decorator);
	    	DocIDMapper mapper = _idxMgr._docIDMapperFactory.getDocIDMapper((ZoieMultiReader<R>)finalReader);
	    	finalReader.setDocIDMapper(mapper);
	        return finalReader;
	      }
	      catch(IOException ioe)
	      {
	        // if reader decoration fails, still need to close the source reader
	        if (srcReader!=null)
	        {
	        	srcReader.close();
	        }
	        throw ioe;
	      }
	    }
	    else{
	      return null;            // null indicates no index exist, following the contract
	    }
	  }

	  public IndexWriter openIndexWriter(Analyzer analyzer,Similarity similarity)
	    throws IOException
	  {
	    if(_indexWriter != null) return _indexWriter;
	    
	    // if index does not exist, create empty index
	    boolean create = !IndexReader.indexExists(_directory); 
	    IndexWriter idxWriter = new IndexWriter(_directory, analyzer, create, MaxFieldLength.UNLIMITED); 
	    // TODO disable compound file for RAMDirecory when lucene bug is fixed
	    idxWriter.setUseCompoundFile(false);
	    idxWriter.setMergeScheduler(_mergeScheduler);
	    idxWriter.setRAMBufferSizeMB(3);
	    
	    if (similarity != null)
	    {
	      idxWriter.setSimilarity(similarity);
	    }
	    _indexWriter = idxWriter;
	    return idxWriter;
	  }
	  
	  @Override
	  public void refresh() throws IOException
	  {
	    synchronized(this)
	    {
	      ZoieIndexReader<R> reader = null;
	      if (_currentReader==null)
	      {
	        reader = openIndexReaderInternal();
	      }
	      else
	      {
	        reader = (ZoieIndexReader<R>)_currentReader.reopen(true);
	        DocIDMapper mapper = _idxMgr._docIDMapperFactory.getDocIDMapper((ZoieMultiReader<R>)reader);
	        reader.setDocIDMapper(mapper);
	      }
	      
	      _currentReader = reader;
	      LongSet delDocs = _delDocs;
          clearDeletes();
          markDeletes(delDocs); // re-mark deletes
          commitDeletes();
	    }
	  }
}
