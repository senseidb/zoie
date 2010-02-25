package proj.zoie.solr;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.IndexReaderFactory;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieSolrIndexReaderFactory<V> extends IndexReaderFactory {
	private ZoieSystem<IndexReader,V> _zoieSystem = null;
	
	private ZoieIndexableInterpreter<V> _interpreter = null;
	private int _batchSize;
	private long _batchDelay;
	private boolean _realtime;
	private Analyzer _analyzer;
	private Similarity _similarity;
	
	@Override
	public void init(NamedList args) {
		super.init(args);
		_interpreter = (ZoieIndexableInterpreter<V>)args.get("interpreter");
		if (_interpreter == null){
			_interpreter = new NopInterpreter<V>();
		}
		try{
		  _batchSize = Integer.parseInt((String)args.get("batchSize"));
		}
		catch(Exception e){
			_batchSize = 10000;
		}
		try{
		  _batchDelay = Long.parseLong((String)args.get("batchDelay"));
		}
		catch(Exception e){
			_batchDelay = 300000;
		}
		try{
		  _realtime = Boolean.parseBoolean((String)args.get("realTime"));
		}
		catch(Exception e){
		  _realtime = false;
		}
		_analyzer = (Analyzer)args.get("analyzer");
		if (_analyzer==null){
			_analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		}
		_similarity = (Similarity)args.get("similarity");
		if (_similarity == null){
			_similarity = new DefaultSimilarity();
		}
	}

	@Override
	public IndexReader newReader(Directory dir, boolean readOnly)
			throws IOException {
		if (_zoieSystem==null){
		  synchronized(this){
			  if (_zoieSystem==null){
				  if (dir instanceof FSDirectory){
				    FSDirectory fsdir = (FSDirectory)dir;
				    _zoieSystem = ZoieSystem.buildDefaultInstance(fsdir.getFile(),_interpreter,
				    							 _analyzer,_similarity,_batchSize,_batchDelay,_realtime);
				    _zoieSystem.start();
				  }
				  else{
					throw new IOException("directory not instance of "+FSDirectory.class); 
				  }
			  }
		  }
		}
		List<ZoieIndexReader<IndexReader>> readerList = _zoieSystem.getIndexReaders();
		return ZoieIndexReader.mergeIndexReaders(readerList);
	}

	@Override
	protected void finalize() throws Throwable {
		try{
			if (_zoieSystem!=null){
				_zoieSystem.shutdown();
			}
		}
		finally{
			super.finalize();
		}
	}

}
