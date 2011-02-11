package proj.zoie.solr;

import java.io.File;
import java.io.Serializable;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;
import org.apache.solr.core.IndexReaderFactory;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.ZoieException;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.mbean.ZoieSystemAdmin;

public class ZoieSystemHome {
	private static Logger log = Logger.getLogger(ZoieSystemHome.class);
	
	private ZoieSystem<IndexReader,DocumentWithID, DefaultZoieVersion, Serializable> _zoieSystem;
	
	private ZoieSystemHome(SolrCore core){
		String idxDir = core.getIndexDir();
		File idxFile = new File(idxDir);
		Analyzer analyzer = null;
		
		try{
			analyzer = core.getSchema().getAnalyzer();
		}
		catch(Exception e){
			log.error(e.getMessage()+", defaulting to "+StandardAnalyzer.class,e);
			analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		}
		
		Similarity similarity = null;
		try{
			similarity = core.getSchema().getSimilarity();
		}
		catch(Exception e){
			log.error(e.getMessage()+", defaulting to "+DefaultSimilarity.class,e);
			similarity = new DefaultSimilarity();
		}
		
		SolrConfig config = core.getSolrConfig();
	
		int batchSize = config.getInt("zoie.batchSize",1000);
		long batchDelay = config.getInt("zoie.batchDelay",300000);
		boolean realtime = config.getBool("zoie.realtime", true);
		
		ZoieConfig<DefaultZoieVersion> zoieConfig = new ZoieConfig<DefaultZoieVersion>(new DefaultZoieVersionFactory());
		zoieConfig.setBatchSize(batchSize);
		zoieConfig.setBatchDelay(batchDelay);
		zoieConfig.setRtIndexing(realtime);
		
		_zoieSystem = new ZoieSystem<IndexReader,DocumentWithID, DefaultZoieVersion, Serializable>(idxFile,new ZoieSolrIndexableInterpreter(),new DefaultIndexReaderDecorator(),zoieConfig);
		
		log.info("Zoie System loaded with: ");
		log.info("zoie.batchSize: "+batchSize);
		log.info("zoie.batchDelay: "+batchDelay);
		log.info("zoie.realtime: "+realtime);
		
		_zoieSystem.start();
		
		log.info("Zoie System started ... ");
		
		IndexReaderFactory readerFactory = core.getIndexReaderFactory();
		if (readerFactory!=null && readerFactory instanceof ZoieSolrIndexReaderFactory){
			((ZoieSolrIndexReaderFactory)readerFactory).setZoieSystem(_zoieSystem);
		}
		
		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		try {
			mbeanServer.registerMBean(new ZoieSystemAdmin(_zoieSystem), new ObjectName("zoie-solr:name=zoie-system"));
		} catch (Exception e) {
			log.error(e.getMessage(),e);
		}
	}
	
	public ZoieSystem<IndexReader,DocumentWithID, DefaultZoieVersion, Serializable> getZoieSystem(){
		return _zoieSystem;
	}
	
	public void shutdown(){
		try{
		  _zoieSystem.flushEvents(10000);
		}
		catch(ZoieException e){
		  log.error(e.getMessage(),e);
		}
		finally{
		  _zoieSystem.shutdown();
		}
	}
	
	protected void finalize(){
		shutdown();
	}
	
	private static ZoieSystemHome instance = null;
	
	public static ZoieSystemHome getInstance(SolrCore core){
		if (instance==null){
			synchronized(ZoieSystemHome.class){
				if (instance == null){
					instance = new ZoieSystemHome(core);
				}
			}
		}
		return instance;
	}
}
