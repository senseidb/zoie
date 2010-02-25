package proj.zoie.perf.indexing;

import java.io.EOFException;
import java.io.IOException;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

public class ContentSourceStreamProvider extends StreamDataProvider<ContentDoc> {
	private static final Logger log = Logger.getLogger(ContentSourceStreamProvider.class);
	private ContentSource _contentSource;
	private long _version;
	private boolean _looping;
	
	public ContentSourceStreamProvider(ContentSource contentSource){
		_contentSource = contentSource;
		_version = 0L;
	}
	
	public void setLooping(boolean looping){
		_looping = looping;
	}
	
	@Override
	public DataEvent<ContentDoc> next(){
		DataEvent<ContentDoc> event = next();
		if (event == null && _looping){
			reset();
			return next();
		}
		else{
			return null;
		}
	}
	private DataEvent<ContentDoc> getNext() {
		ContentDoc dataNode = null;
		try{
			dataNode = new ContentDoc();
			dataNode = _contentSource.getNextDocData(dataNode);
			
			// for the purpose of perf test, ID's are normalized to version
			dataNode.setID(_version);
			if (dataNode == null) return null;
		}
		catch(EOFException eof){
			return null;
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
		
		DataEvent<ContentDoc> event = new DataEvent<ContentDoc>(_version++,dataNode);
		return event;
	}

	@Override
	public void reset() {
		try {
			_contentSource.resetInputs();
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
	}

}
