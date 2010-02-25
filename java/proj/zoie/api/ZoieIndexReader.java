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
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.FilterIndexReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ReaderUtil;

import proj.zoie.api.impl.DefaultIndexReaderMerger;
import proj.zoie.api.indexing.IndexReaderDecorator;

public abstract class ZoieIndexReader<R extends IndexReader> extends FilterIndexReader {
	public static final long DELETED_UID = Long.MIN_VALUE;
	
	protected ThreadLocal<int[]> _delDocIds;
	protected long _minUID;
	protected long _maxUID;
	protected boolean _noDedup = false;
    protected final IndexReaderDecorator<R> _decorator;
    protected DocIDMapper _docIDMapper;
    
    public static <R extends IndexReader> List<R> extractDecoratedReaders(List<ZoieIndexReader<R>> readerList) throws IOException{
  	  LinkedList<R> retList = new LinkedList<R>();
  	  for (ZoieIndexReader<R> reader : readerList){
  		  retList.addAll(reader.getDecoratedReaders());
  	  }
  	  return retList;
  	}
  	
  	public static class SubReaderInfo<R extends IndexReader>{
	  public final R subreader;
	  public final int subdocid;

	  SubReaderInfo(R subreader,int subdocid){
		this.subreader = subreader;
		this.subdocid = subdocid;
	  }
  	}
  	
  	public interface SubReaderAccessor<R extends IndexReader>{
  		SubReaderInfo<R> getSubReaderInfo(int docid);
  	}
  	
  	public interface SubZoieReaderAccessor<R extends IndexReader>{
  		SubReaderInfo<ZoieIndexReader<R>> getSubReaderInfo(int docid);
  		SubReaderInfo<ZoieIndexReader<R>> geSubReaderInfoFromUID(long uid);
  	}
  	  
  	public static <R extends IndexReader> SubReaderAccessor<R> getSubReaderAccessor(List<R> readerList){
  	  int size = readerList.size();
  	  final IndexReader[] subR = new IndexReader[size];
  	  final int[] starts = new int[size+1];
  	  
  	  int maxDoc = 0;

  	  int i = 0;
  	  for (R r : readerList) {
  	    starts[i] = maxDoc;
  	    subR[i] = r;
  	    maxDoc += r.maxDoc();          // compute maxDocs
  	    i++;
  	  }
  	  starts[size] = maxDoc;
  	  
  	  return new SubReaderAccessor<R>() {
		
		public SubReaderInfo<R> getSubReaderInfo(int docid) {
			int subIdx = ReaderUtil.subIndex(docid, starts);
		  	int subdocid = docid - starts[subIdx];
		  	R subreader = (R)(subR[subIdx]);
		  	return new SubReaderInfo<R>(subreader, subdocid);
		}
	  };
  	}
	
  	public static <R extends IndexReader> SubZoieReaderAccessor<R> getSubZoieReaderAccessor(List<ZoieIndexReader<R>> readerList){
    	  int size = readerList.size();
    	  final ZoieIndexReader<?>[] subR = new ZoieIndexReader<?>[size];
    	  final int[] starts = new int[size+1];
    	  
    	  int maxDoc = 0;

    	  int i = 0;
    	  for (ZoieIndexReader<R> r : readerList) {
    	    starts[i] = maxDoc;
    	    subR[i] = r;
    	    maxDoc += r.maxDoc();          // compute maxDocs
    	    i++;
    	  }
    	  starts[size] = maxDoc;
    	  
    	  return new SubZoieReaderAccessor<R>() {
  		
	  		public SubReaderInfo<ZoieIndexReader<R>> getSubReaderInfo(int docid) {
	  			int subIdx = ReaderUtil.subIndex(docid, starts);
	  		  	int subdocid = docid - starts[subIdx];
	  		  	return new SubReaderInfo<ZoieIndexReader<R>>((ZoieIndexReader<R>)(subR[subIdx]), subdocid);
	  		}

			public SubReaderInfo<ZoieIndexReader<R>> geSubReaderInfoFromUID(long uid) {
				SubReaderInfo<ZoieIndexReader<R>> info = null;
				for (int i=0;i<subR.length;++i){
					ZoieIndexReader<R> subReader = (ZoieIndexReader<R>)subR[i];
					DocIDMapper mapper = subReader.getDocIDMaper();
					int docid = mapper.getDocID(uid);
					if (docid!=DocIDMapper.NOT_FOUND){
						info = new SubReaderInfo<ZoieIndexReader<R>>(subReader,docid);
						break;
					}
				}
				return info;
			}
	  	  };
    	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r) throws IOException{
		return open(r,null);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r,IndexReaderDecorator<R> decorator) throws IOException{
		return new ZoieMultiReader<R>(r,decorator);
	}
	
	public static <R extends IndexReader> ZoieIndexReader<R> open(Directory dir,IndexReaderDecorator<R> decorator) throws IOException{
		IndexReader r = IndexReader.open(dir, true);
		// load zoie reader
		try{
			return open(r,decorator);
		}
		catch(IOException ioe){
			if (r!=null){
				r.close();
			}
			throw ioe;
		}
	}
	
	public static <R extends IndexReader,T extends IndexReader> R mergeIndexReaders(List<ZoieIndexReader<T>> readerList,IndexReaderMerger<R,T> merger){
		return merger.mergeIndexReaders(readerList);
	}
	
	public static <T extends IndexReader> MultiReader mergeIndexReaders(List<ZoieIndexReader<T>> readerList){
		return mergeIndexReaders(readerList,new DefaultIndexReaderMerger<T>());
	}
		
	protected ZoieIndexReader(IndexReader in,IndexReaderDecorator<R> decorator) throws IOException
	{
		super(in);
		_decorator = decorator;
		_delDocIds=new ThreadLocal<int[]>();
		_minUID=Long.MAX_VALUE;
		_maxUID=0;
	}
	
	abstract public List<R> getDecoratedReaders() throws IOException;
    abstract public void setDelDocIds();
    abstract public void markDeletes(LongSet delDocs, LongSet deltedUIDs);
    abstract public void commitDeletes();
	     
	public IndexReader getInnerReader(){
		return in;
	}
	
	@Override
	public boolean hasDeletions()
	{
	  if(!_noDedup)
	  {
		int[] delSet = _delDocIds.get();
	    if(delSet != null && delSet.length > 0) return true;
	  }
	  return in.hasDeletions();
	}
	
	protected abstract boolean hasIndexDeletions();
	
	public boolean hasDuplicates()
	{
		int[] delSet = _delDocIds.get();
		return (delSet!=null && delSet.length > 0);
	}

	@Override
	abstract public boolean isDeleted(int docid);
	
	public boolean isDuplicate(int docid)
	{
	  int[] delSet = _delDocIds.get();
	  return delSet!=null && Arrays.binarySearch(delSet, docid) >= 0;
	}
	
	public boolean isDuplicateUID(long uid){
	  int docid = _docIDMapper.getDocID(uid);
	  return isDuplicate(docid);
	}
	
	public int[] getDelDocIds()
	{
	  return _delDocIds.get();
	}
	
	public long getMinUID()
	{
		return _minUID;
	}
	
	public long getMaxUID()
	{
		return _maxUID;
	}

	abstract public long getUID(int docid);
	
	public DocIDMapper getDocIDMaper(){
		return _docIDMapper;
	}
	
	public void setDocIDMapper(DocIDMapper docIDMapper){
		_docIDMapper = docIDMapper;
	}
	
	public void setNoDedup(boolean noDedup)
	{
	  _noDedup = noDedup;
	}

	@Override
	abstract public ZoieIndexReader<R>[] getSequentialSubReaders();
	
	@Override
	abstract public TermDocs termDocs() throws IOException;
	
	@Override
	abstract public TermPositions termPositions() throws IOException;
	
}
