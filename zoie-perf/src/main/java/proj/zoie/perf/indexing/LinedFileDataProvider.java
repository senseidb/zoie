package proj.zoie.perf.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.perf.client.ZoiePerfVersion;

public class LinedFileDataProvider extends StreamDataProvider<String> {

	private static final Logger logger = Logger.getLogger(LinedFileDataProvider.class);
	
	private final File _file;
	private long _startingOffset;
	private long _offset;
	private int _count = 0;
	
	private BufferedReader _rad;
	
	
	public LinedFileDataProvider(File file,long startingOffset){
      super(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
	  _file = file;
	  _rad = null;
	  _startingOffset = startingOffset;
	}
	
	
	@Override
	public DataEvent<String> next() {
		DataEvent<String> event = null;
		if (_rad!=null){
		  try{
			String line = _rad.readLine();
			if (line == null) return null;
			
			String version = ZoiePerfVersion.toString(_count,_offset);
			_offset+=version.length();
			
			event = new DataEvent<String>(line,version);
		  }
		  catch(IOException ioe){
			logger.error(ioe.getMessage(),ioe);
		  }
		}
		_count++;
		return event;
	}
	
	public int getCount(){
		return _count;
	}

	@Override
	public void setStartingOffset(String version) {
		ZoiePerfVersion perfVersion = ZoiePerfVersion.fromString(version);
		_startingOffset = perfVersion.offsetVersion;
	}

	@Override
	public void reset() {
	  
	  try {
		if (_rad!=null){
		  _rad.close();
		}
		_rad = new BufferedReader(new InputStreamReader(new FileInputStream(_file),Charset.forName("UTF-8")));
		_offset = _startingOffset;
		for (long i=0;i<_offset;++i){
			_rad.read();
		}
		_count = 0;
	  } catch (IOException e) {
			logger.error(e.getMessage(),e);
	  }
	}

	@Override
	public void start() {
		super.start();
		reset();
	}

	@Override
	public void stop() {
		try{
		  super.stop();
		  
		}
		finally{
			try{
			    if (_rad!=null){
			    	_rad.close();
			    }
			  }
			  catch(IOException ioe){
				logger.error(ioe.getMessage(),ioe);
			  }
			  finally{
			    _rad = null;
			  }
		}
	}	
}
