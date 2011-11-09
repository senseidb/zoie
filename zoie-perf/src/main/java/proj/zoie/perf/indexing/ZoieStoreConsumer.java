package proj.zoie.perf.indexing;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.LifeCycleCotrolledDataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.store.ZoieStore;

public class ZoieStoreConsumer implements LifeCycleCotrolledDataConsumer<String> {

	private static final Logger logger = Logger.getLogger(ZoieStoreConsumer.class);
	private final ZoieStore _store;
	
	public ZoieStoreConsumer(ZoieStore store){
		_store = store;
	}
	@Override
	public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<String>> data)
			throws ZoieException {
		for (DataEvent<String> datum : data){
			String version = datum.getVersion();
			String jsonString = datum.getData();
			
			try{
			  byte[] bytes = jsonString.getBytes(Charset.forName("UTF-8"));
			  JSONObject obj = new JSONObject(jsonString);
			  long id = Long.parseLong(obj.getString("id"));
			  _store.put(id, bytes, version);
			}
			catch(Exception e){
				System.out.println("Skipping: "+jsonString);
			}
		}
		
	}

	@Override
	public String getVersion() {
		return _store.getVersion();
	}

	@Override
	public void start() {
		
	}

	@Override
	public void stop() {
		try {
			_store.close();
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}
	}

}
