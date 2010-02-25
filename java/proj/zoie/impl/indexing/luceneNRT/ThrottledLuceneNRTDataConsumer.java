package proj.zoie.impl.indexing.luceneNRT;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import proj.zoie.api.DataConsumer;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public class ThrottledLuceneNRTDataConsumer<V> implements DataConsumer<V>,IndexReaderFactory<IndexReader>{
	private static final Logger logger = Logger.getLogger(ThrottledLuceneNRTDataConsumer.class);

	private static int MAX_READER_GENERATION = 3;
	/**
	 * document ID field name
	*/
	public static final String DOCUMENT_ID_FIELD = "id";
	  
	
	private IndexWriter _writer;
	private Analyzer _analyzer;
	private ZoieIndexableInterpreter<V> _interpreter;
	private Directory _dir;
	private final long _throttleFactor;
	private IndexReader _currentReader;
	private ReopenThread _reopenThread;
	private HashSet<IndexReader> _returnSet = new HashSet<IndexReader>();
	private ConcurrentLinkedQueue<IndexReader> _returnList = new ConcurrentLinkedQueue<IndexReader>();
	
	public ThrottledLuceneNRTDataConsumer(File dir,ZoieIndexableInterpreter<V> interpreter,long throttleFactor) throws IOException{
		this(FSDirectory.open(dir),new StandardAnalyzer(Version.LUCENE_CURRENT),interpreter,throttleFactor);
	}
	
	public ThrottledLuceneNRTDataConsumer(File dir,Analyzer analyzer,ZoieIndexableInterpreter<V> interpreter,long throttleFactor) throws IOException{
		this(FSDirectory.open(dir),analyzer,interpreter,throttleFactor);
	}
	
	public ThrottledLuceneNRTDataConsumer(Directory dir,Analyzer analyzer,ZoieIndexableInterpreter<V> interpreter,long throttleFactor){
		_writer = null;
		_analyzer = analyzer;
		_interpreter = interpreter;
		_dir = dir;
		_throttleFactor = throttleFactor;
		_currentReader = null;
		if (_throttleFactor<=0) throw new IllegalArgumentException("throttle factor must be > 0");
		_reopenThread = new ReopenThread();
	}
	
	public void start(){
		try {
			_writer = new IndexWriter(_dir, _analyzer,MaxFieldLength.UNLIMITED);
			_reopenThread.start();
		} catch (IOException e) {
			logger.error("uanble to start consumer: "+e.getMessage(),e);
		}
	}
	
	public void shutdown(){
		_reopenThread.terminate();
		if (_currentReader!=null){
			try {
				_currentReader.close();
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		}
		if (_writer!=null){
			try {
				_writer.close();
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		}
	}
	
	public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<V>> events)
			throws ZoieException {
		if (_writer == null){
			throw new ZoieException("Internal IndexWriter null, perhaps not started?");
		}
		
		if (events.size() > 0){
			for (DataEvent<V> event : events){
				ZoieIndexable indexable = _interpreter.convertAndInterpret(event.getData());
				if (indexable.isSkip()) continue;
				
				try {
				  _writer.deleteDocuments(new Term(DOCUMENT_ID_FIELD,String.valueOf(indexable.getUID())));
				} catch(IOException e) {
				  throw new ZoieException(e.getMessage(),e);
				}
				  
			  IndexingReq[] reqs = indexable.buildIndexingReqs();
			  for (IndexingReq req : reqs){
				Analyzer localAnalyzer = req.getAnalyzer();
				Document doc = req.getDocument();
				Field uidField = new Field(DOCUMENT_ID_FIELD,String.valueOf(indexable.getUID()),Store.NO,Index.NOT_ANALYZED_NO_NORMS);
				uidField.setOmitTermFreqAndPositions(true);
				doc.add(uidField);
				if (localAnalyzer == null) localAnalyzer = _analyzer;
				try {
					_writer.addDocument(doc, localAnalyzer);
				} catch(IOException e) {
					throw new ZoieException(e.getMessage(),e);
				}
			  }
			}
			
			
			int numdocs;
			try {
				// for realtime commit is not needed per lucene mailing list
				//_writer.commit();
				numdocs = _writer.numDocs();
			} catch (IOException e) {
				throw new ZoieException(e.getMessage(),e);
			}
			
			logger.info("flushed "+events.size()+" events to index, index now contains "+numdocs+" docs.");
		}
	}

	public Analyzer getAnalyzer() {
		return _analyzer;
	}

	public IndexReader getDiskIndexReader() throws IOException {
		return _currentReader;
	}

	public List<IndexReader> getIndexReaders() throws IOException {
		IndexReader subReader = getDiskIndexReader();
		ArrayList<IndexReader> list = new ArrayList<IndexReader>();
		if (subReader!=null){
			list.add(subReader);
		}
		return list;
	}

	public void returnIndexReaders(List<IndexReader> readers) {
		if (readers!=null){
			for (IndexReader r : readers){
				if (r != _currentReader){
					returnReader(r);
				}
			}
		}
	}
	
	private void returnReader(IndexReader reader){
		synchronized(_returnSet){
			if (!_returnSet.contains(reader)){
				_returnSet.add(reader);
				_returnList.add(reader);
			}
			while (_returnList.size()>=MAX_READER_GENERATION){
				logger.info("remove and close old reader: "+_returnList.size()+"/"+_returnSet.size());
				IndexReader r = _returnList.remove();
				_returnSet.remove(r);
				try {
					r.close();
				} catch (IOException e) {
					logger.error(e.getMessage(),e);
				}
			}
		}
	}
	
	private class ReopenThread extends Thread{
		private boolean _stop;
		ReopenThread(){
			super("reopen thread");
			setDaemon(true);
			_stop=false;
		}
		
		void terminate(){
			if (!_stop){
				_stop=false;
				interrupt();
			}
		}
		
		public void run(){
			while(!_stop){
				try {
					Thread.sleep(ThrottledLuceneNRTDataConsumer.this._throttleFactor);
				} catch (InterruptedException e) {
					continue;
				}
				if (ThrottledLuceneNRTDataConsumer.this._writer!=null){
					try {
						logger.info("updating reader...");
						IndexReader oldReader = ThrottledLuceneNRTDataConsumer.this._currentReader;
						ThrottledLuceneNRTDataConsumer.this._currentReader=ThrottledLuceneNRTDataConsumer.this._writer.getReader();
						if (oldReader!=null){
							returnReader(oldReader);
						}
					} catch (IOException e) {
						logger.error(e.getMessage(),e);
					}
				}
			}
		}
	}
  
  public long getVersion()
  {
    throw new UnsupportedOperationException();
  }
}
