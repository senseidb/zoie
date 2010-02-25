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
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermPositions;

import proj.zoie.api.impl.util.ArrayDocIdSet;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.internal.ZoieSegmentTermDocs;
import proj.zoie.impl.indexing.internal.ZoieSegmentTermPositions;

public class ZoieSegmentReader<R extends IndexReader> extends ZoieIndexReader<R>{
	static final String termVal="_UID";
	static final Term UID_TERM = new Term(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD,termVal);
    private R _decoratedReader;
    private long[] _uidArray;
    private IntRBTreeSet _delDocIdSet = new IntRBTreeSet();
    private int[] _currentDelDocIds;
    
    static class UIDTokenStream extends TokenStream {
        private boolean returnToken = false;

        private PayloadAttribute payloadAttr;
        private TermAttribute termAttr;
        UIDTokenStream(long uid) {
          byte[] buffer = new byte[8];
          buffer[0] = (byte) (uid);
          buffer[1] = (byte) (uid >> 8);
          buffer[2] = (byte) (uid >> 16);
          buffer[3] = (byte) (uid >> 24);
          buffer[4] = (byte) (uid >> 32);
          buffer[5] = (byte) (uid >> 40);
          buffer[6] = (byte) (uid >> 48);
          buffer[7] = (byte) (uid >> 56);
          payloadAttr = (PayloadAttribute)addAttribute(PayloadAttribute.class);
          payloadAttr.setPayload(new Payload(buffer));
          termAttr = (TermAttribute)addAttribute(TermAttribute.class);
          termAttr.setTermBuffer(termVal);
          returnToken = true;
        }

        @Override
		public boolean incrementToken() throws IOException {
        	if (returnToken) {
                returnToken = false;
                return true;
              } else {
                return false;
              }
		}  
    }

	public static void fillDocumentID(Document doc,long id)
	{
      doc.add(new Field(ZoieSegmentReader.UID_TERM.field(), new UIDTokenStream(id))); 
	}

	public ZoieSegmentReader(IndexReader in, IndexReaderDecorator<R> decorator)
			throws IOException {
		super(in,decorator);
		if (!(in instanceof SegmentReader)){
			throw new IllegalStateException("ZoieSegmentReader can only be constucted from "+SegmentReader.class);
		}
		init(in);
		_decoratedReader = (decorator == null ? null : decorator.decorate(this));
	}
	
	ZoieSegmentReader(ZoieSegmentReader<R> copyFrom,IndexReader innerReader) throws IOException{
		super(innerReader,copyFrom._decorator);
		_uidArray = copyFrom._uidArray;
		_maxUID = copyFrom._maxUID;
		_minUID = copyFrom._minUID;
		_noDedup = copyFrom._noDedup;
		_docIDMapper = copyFrom._docIDMapper;
		_delDocIdSet = copyFrom._delDocIdSet;
		
		if (copyFrom._decorator == null){
			_decoratedReader = null;
		}
		else{
			_decoratedReader = copyFrom._decorator.redecorate(copyFrom._decoratedReader, this);
		}
	}
	
	@Override
	public void markDeletes(LongSet delDocs, LongSet deletedUIDs)
	{
      DocIDMapper idMapper = getDocIDMaper();
      LongIterator iter = delDocs.iterator();
      IntRBTreeSet delDocIdSet = _delDocIdSet;

      while(iter.hasNext())
      {
        long uid = iter.nextLong();
        if (ZoieIndexReader.DELETED_UID != uid)
        {
          int docid = idMapper.getDocID(uid);
          if(docid != DocIDMapper.NOT_FOUND)
          {
            delDocIdSet.add(docid);
            deletedUIDs.add(uid);
          }
        }
      }	  
	}
	
	@Override
	public void commitDeletes()
	{
	  _currentDelDocIds = _delDocIdSet.toIntArray();
	}
	
	public void setDelDocIds()
	{
	  _delDocIds.set(_currentDelDocIds);
	}
	
	public R getDecoratedReader(){
		return _decoratedReader;
	}
	
	@Override
	public List<R> getDecoratedReaders()
    {
	  ArrayList<R> list = new ArrayList<R>(1);
	  if (_decoratedReader!=null){
	    list.add(_decoratedReader);
	  }
	  return list;
    }
	
	private void init(IndexReader reader) throws IOException
	{
		int maxDoc = reader.maxDoc();
		_uidArray = new long[maxDoc]; 
		TermPositions tp = null;
		byte[] payloadBuffer = new byte[8];       // four bytes for a long
		try
		{
          tp = reader.termPositions(UID_TERM);
          int idx = 0;
          while (tp.next())
          {
            int doc = tp.doc();
            assert doc < maxDoc;
            
            while(idx < doc) _uidArray[idx++] = DELETED_UID; // fill the gap
            
            tp.nextPosition();
            tp.getPayload(payloadBuffer, 0);
            long uid = bytesToLong(payloadBuffer);
            if(uid < _minUID) _minUID = uid;
            if(uid > _maxUID) _maxUID = uid;
            _uidArray[idx++] = uid;
    	  }
          while(idx < maxDoc) _uidArray[idx++] = DELETED_UID; // fill the gap
		}
		finally
		{
          if (tp!=null)
          {
        	  tp.close();
          }
		}
	}
	
	private static long bytesToLong(byte[] bytes){
        return ((long)(bytes[7] & 0xFF) << 56) | ((long)(bytes[6] & 0xFF) << 48) | ((long)(bytes[5] & 0xFF) << 40) | ((long)(bytes[4] & 0xFF) << 32) | ((long)(bytes[3] & 0xFF) << 24) | ((long)(bytes[2] & 0xFF) << 16)
           | ((long)(bytes[1] & 0xFF) <<  8) |  (long)(bytes[0] & 0xFF);
	}
	
	@Override
	public long getUID(int docid)
	{
		return _uidArray[docid];
	}

	public long[] getUIDArray()
	{
		return _uidArray;
	}

	@Override
	protected boolean hasIndexDeletions(){
		return in.hasDeletions();
	}
	
	@Override
	public boolean isDeleted(int docid)
	{
	  if(!_noDedup)
	  {
		int[] delSet = _delDocIds.get();
	    if(delSet != null && Arrays.binarySearch(delSet, docid) >= 0) return true;
	  }
	  return in.isDeleted(docid);
	}
	
	@Override
	public TermDocs termDocs(Term term) throws IOException {
		 ensureOpen();
		 TermDocs td = in.termDocs(term);
		 if(_noDedup) return td;
		  
		 int[] delDocIds = _delDocIds.get();
		 if(td == null || delDocIds == null || delDocIds.length == 0) return td;
	     return new ZoieSegmentTermDocs(td, new ArrayDocIdSet(delDocIds));
	}

	@Override
	public TermDocs termDocs() throws IOException
	{
	  ensureOpen();
	  TermDocs td = in.termDocs();
	  if(_noDedup) return td;
	  
	  int[] delDocIds = _delDocIds.get();
	  if(td == null || delDocIds == null || delDocIds.length == 0) return td;
      
      return new ZoieSegmentTermDocs(td, new ArrayDocIdSet(delDocIds));
	}
	
	@Override
	public TermPositions termPositions(Term term) throws IOException {
		ensureOpen();
		  TermPositions tp = in.termPositions(term);
	      if(_noDedup) return tp;
	      
	      int[] delDocIds = _delDocIds.get();
	      if(tp == null || delDocIds == null || delDocIds.length == 0) return tp;
	      
	      return new ZoieSegmentTermPositions(tp, new ArrayDocIdSet(delDocIds));
	}

	@Override
	public TermPositions termPositions() throws IOException
	{
	  ensureOpen();
	  TermPositions tp = in.termPositions();
      if(_noDedup) return tp;
      
      int[] delDocIds = _delDocIds.get();
      if(tp == null || delDocIds == null || delDocIds.length == 0) return tp;
      
      return new ZoieSegmentTermPositions(tp, new ArrayDocIdSet(delDocIds));
	}

	@Override
	public ZoieIndexReader<R>[] getSequentialSubReaders() {
		return null;
	}
	
	public String getSegmentName(){
		return ((SegmentReader)in).getSegmentName();
	}

	@Override
	protected void doClose() throws IOException {
		
	}

	@Override
	public synchronized void decRef() throws IOException {
		
	}
}
