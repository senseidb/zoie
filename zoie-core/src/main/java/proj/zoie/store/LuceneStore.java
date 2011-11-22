package proj.zoie.store;

import it.unimi.dsi.fastutil.longs.Long2IntRBTreeMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.ZoieMergePolicy;

public class LuceneStore extends AbstractZoieStore {
	
	private static final String VERSION_NAME = "version";
	
	private static final Logger logger = Logger.getLogger(LuceneStore.class);
	
	
	private static class ReaderData{
		final IndexReader reader;
		final Long2IntRBTreeMap uidMap;
		final long _minUID;
		final long _maxUID;
		ReaderData(IndexReader reader) throws IOException{
			this.reader = reader;
			long minUID = Long.MAX_VALUE;
			long maxUID = Long.MIN_VALUE;
			
			uidMap = new Long2IntRBTreeMap();
			uidMap.defaultReturnValue(-1);
			int maxDoc = reader.maxDoc();
			if (maxDoc == 0){
				_minUID = Long.MIN_VALUE;
				_maxUID = Long.MIN_VALUE;
				return;
			}
			TermPositions tp = null;
			byte[] payloadBuffer = new byte[8];       // four bytes for a long
			try
			{
	          tp = reader.termPositions(ZoieSegmentReader.UID_TERM);
	          while (tp.next())
	          {
	            int doc = tp.doc();
	            assert doc < maxDoc;
	            
	            tp.nextPosition();
	            tp.getPayload(payloadBuffer, 0);
	            long uid = ZoieSegmentReader.bytesToLong(payloadBuffer);
	            if(uid < minUID) minUID = uid;
	            if(uid > maxUID) maxUID = uid;
	            uidMap.put(uid, doc);
	    	  }
			}
			finally
			{
	          if (tp!=null)
	          {
	        	  tp.close();
	          }
			}
			
			_minUID = minUID;
			_maxUID = maxUID;
		}
		
		void close(){
			if (this.reader!=null){
		  	  try {
				this.reader.close();
			  } catch (IOException e) {
				logger.error(e.getMessage(),e);
			  }
			}
			
		}
	}
	
	private final String _field;
	private final Directory _dir;
	private IndexWriter _idxWriter;
	private volatile ReaderData _currentReaderData;
	private volatile ReaderData _oldReaderData;
	private volatile boolean _closed = true;
	
	private LuceneStore(Directory dir,String field) throws IOException{
		_field = field;
		_idxWriter = null;
		_dir = dir;
	}
	
	public void open() throws IOException{
		if (_closed){
		  IndexWriterConfig idxWriterConfig = new IndexWriterConfig(Version.LUCENE_34,new StandardAnalyzer(Version.LUCENE_34));
		  idxWriterConfig.setMergePolicy(new ZoieMergePolicy());
		  idxWriterConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
		  _idxWriter = new IndexWriter(_dir,idxWriterConfig);
		  updateReader();
		  _closed = false;
		}
	}
	
	private void updateReader() throws IOException{

		IndexReader oldReader = null;
		
		if (_currentReaderData!=null){
			oldReader = _currentReaderData.reader;
		}
		
		IndexReader idxReader = IndexReader.open(_idxWriter, true);

		// if reader did not change, no updates were applied, not need to refresh
		if (idxReader == oldReader) return;
		

		ReaderData readerData = new ReaderData(idxReader);
		
		_currentReaderData = readerData;
		if (_oldReaderData!=null){
			ReaderData tmpOld = _oldReaderData;
			_oldReaderData = _currentReaderData;
			tmpOld.close();
		}
		_currentReaderData = readerData;
	}
	
	public static ZoieStore openStore(Directory idxDir,String field,boolean compressionOff) throws IOException{
		LuceneStore store = new LuceneStore(idxDir,field);
		store.setDataCompressed(!compressionOff);
		store.open();
		return store;
	}
	
	private int mapDocId(long uid){
		if (_currentReaderData!=null){
			if (_currentReaderData._maxUID>=uid && _currentReaderData._minUID<=uid){
				return _currentReaderData.uidMap.get(uid);
			}
		}
		return -1;
	}

	@Override
	protected void persist(long uid, byte[] data) throws IOException {
		Document doc = new Document();
		doc.add(new Field(_field,data));
		ZoieSegmentReader.fillDocumentID(doc, uid);
		_idxWriter.addDocument(doc);
	}

	@Override
	protected void persistDelete(long uid) throws IOException {
		final int docid = mapDocId(uid);
		if (docid<0) return;
		
		Query deleteQ = new ConstantScoreQuery(new Filter(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
				return new DocIdSet(){

					@Override
					public DocIdSetIterator iterator() throws IOException {
						return new DocIdSetIterator() {
							int currId = -1;
							
							@Override
							public int nextDoc() throws IOException {
								if (currId == -1){
									currId = docid;
								}
								else{
									currId = DocIdSetIterator.NO_MORE_DOCS;
								}
								return currId;
							}
							
							@Override
							public int docID() {
								return currId;
							}
							
							@Override
							public int advance(int target) throws IOException {
								if (currId!=DocIdSetIterator.NO_MORE_DOCS){
									if (target<docid){
										currId = docid;
									}
									else{
										currId = DocIdSetIterator.NO_MORE_DOCS;
									}
								}
								return currId;
							}
						};
					}
					
				};
			}
			
		});
		_idxWriter.deleteDocuments(deleteQ);
		if (_currentReaderData!=null){
		  _currentReaderData.uidMap.remove(uid);
		}

	}

	@Override
	protected byte[] getFromStore(long uid) throws IOException {
		int docid = mapDocId(uid);
		if (docid<0) return null;
		IndexReader reader = null;
		if (_currentReaderData!=null){
			reader = _currentReaderData.reader;
		}
		if (docid>=0 && reader!=null){
			Document doc = reader.document(docid);
			if (doc!=null){
			  return doc.getBinaryValue(_field);
			}
		}
		return null;
	}

	@Override
	protected void commitVersion(String version) throws IOException {
		HashMap<String,String> versionMap = new HashMap<String,String>();
        versionMap.put(VERSION_NAME, version);
        _idxWriter.commit(versionMap);
        updateReader();
	}

	@Override
	public String getPersistedVersion() throws IOException {
		IndexReader reader = null;
		if (_currentReaderData!=null){
			reader = _currentReaderData.reader;
		}
		if (reader!=null){
		  Map<String,String> versionMap = reader.getCommitUserData();
		  return versionMap.get(VERSION_NAME);
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		if (!_closed){
		  _idxWriter.close();
		  _closed = true;
		}
	}
}
