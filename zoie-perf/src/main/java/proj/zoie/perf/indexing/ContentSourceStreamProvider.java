package proj.zoie.perf.indexing;

import java.io.EOFException;
import java.io.IOException;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.impl.indexing.StreamDataProvider;

public class ContentSourceStreamProvider extends StreamDataProvider<ContentDoc, DefaultZoieVersion> {
	private static final Logger log = Logger.getLogger(ContentSourceStreamProvider.class);
	private ContentSource _contentSource;
	private DefaultZoieVersion _version;
	private boolean _looping;
	
	public ContentSourceStreamProvider(ContentSource contentSource){
		_contentSource = contentSource;
		_version = null;
	}
	
	public void setLooping(boolean looping){
		_looping = looping;
	}
	
	@Override
	public DataEvent<ContentDoc, DefaultZoieVersion> next(){
		DataEvent<ContentDoc, DefaultZoieVersion> event = next();
		if (event == null && _looping){
			reset();
			return next();
		}
		else{
			return null;
		}
	}
	private DataEvent<ContentDoc, DefaultZoieVersion> getNext() {
		ContentDoc dataNode = null;
		try{
			dataNode = new ContentDoc();
			dataNode = _contentSource.getNextDocData(dataNode);
			
			// for the purpose of perf test, ID's are normalized to version
			dataNode.setID(_version.getVersionId());
			if (dataNode == null) return null;
		}
		catch(EOFException eof){
			return null;
		} catch (IOException e) {
			log.error(e.getMessage(),e);
		}
		DefaultZoieVersion v = new DefaultZoieVersion();
		v.setVersionId(_version.getVersionId()+1);
		DataEvent<ContentDoc,DefaultZoieVersion> event = new DataEvent<ContentDoc,DefaultZoieVersion>(dataNode, v);
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
