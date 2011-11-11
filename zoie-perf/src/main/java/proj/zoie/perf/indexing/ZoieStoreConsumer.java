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
	private static final int DEFAULT_BATCH_SIZE = 1000;
	private final ZoieStore _store;
	private final int _batchSize;
	private volatile int _count;
	
	public ZoieStoreConsumer(ZoieStore store){
		this(store,DEFAULT_BATCH_SIZE);
	}
	
	public ZoieStoreConsumer(ZoieStore store,int batchSize){
		_store = store;
		if (batchSize<=0){
			_batchSize = DEFAULT_BATCH_SIZE;
		}
		else{
			_batchSize = batchSize;
		}
		_count = 0;
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

			  _count++;
			}
			catch(Exception e){
				// ignore to avoid performance skew
				//System.out.println("Skipping: "+jsonString);
			}
		}

		
		if (_count>=_batchSize){
			try{
			  _store.commit();
			}
			catch(Exception e){
			  throw new ZoieException(e.getMessage(),e);
			}
			finally{
			  _count = 0;
			}
		}
	}

	@Override
	public String getVersion() {
		return _store.getVersion();
	}

	@Override
	public void start() {
		try{
		  _store.open();
		}
		catch(Exception e){
		  logger.error(e.getMessage(),e);
		}
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
