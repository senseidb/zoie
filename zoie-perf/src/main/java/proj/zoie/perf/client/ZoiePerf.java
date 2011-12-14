package proj.zoie.perf.client;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.json.JSONObject;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.LifeCycleCotrolledDataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.SimpleReaderCache;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.impl.indexing.luceneNRT.ThrottledLuceneNRTDataConsumer;
import proj.zoie.perf.indexing.LinedFileDataProvider;
import proj.zoie.perf.indexing.TweetInterpreter;
import proj.zoie.perf.servlet.ZoiePerfServlet;
import proj.zoie.store.LuceneStore;
import proj.zoie.store.ZoieStore;
import proj.zoie.store.ZoieStoreConsumer;
import proj.zoie.store.ZoieStoreSerializer;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.GaugeMetric;
import com.yammer.metrics.core.MeterMetric;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.TimerMetric;
import com.yammer.metrics.reporting.CsvReporter;

public class ZoiePerf {

	static final Charset UTF8 = Charset.forName("UTF-8");
	private static class PerfTestHandler {
		final LifeCycleCotrolledDataConsumer<String> consumer;
		final QueryHandler queryHandler;

		PerfTestHandler(LifeCycleCotrolledDataConsumer<String> consumer,
				QueryHandler queryHandler) {
			this.consumer = consumer;
			this.queryHandler = queryHandler;
		}
	}

	static TweetInterpreter interpreter = new TweetInterpreter();

	static Map<String,DIRECTORY_MODE> modeMap = new HashMap<String,DIRECTORY_MODE>();
	static{
		modeMap.put("file", DIRECTORY_MODE.SIMPLE);
		modeMap.put("mmap", DIRECTORY_MODE.MMAP);
		modeMap.put("nio", DIRECTORY_MODE.NIO);
	}
	
	
	static PerfTestHandler buildZoieHandler(File idxDir, Configuration topConf,Configuration conf) throws Exception {

		ZoieConfig zoieConfig = new ZoieConfig();
		zoieConfig.setAnalyzer(new StandardAnalyzer(Version.LUCENE_34));
		zoieConfig.setBatchSize(100000);
		zoieConfig.setBatchDelay(10000);
		zoieConfig.setMaxBatchSize(100000);
		zoieConfig.setRtIndexing(true);
		zoieConfig.setVersionComparator(ZoiePerfVersion.COMPARATOR);
		zoieConfig.setReadercachefactory(SimpleReaderCache.FACTORY);
		
		String modeConf = topConf.getString("perf.directory.type", "file");
		DIRECTORY_MODE mode = modeMap.get(modeConf);
		if (mode == null) mode = DIRECTORY_MODE.SIMPLE;
		DirectoryManager dirMgr = new DefaultDirectoryManager(idxDir,mode);

		IndexReaderDecorator<IndexReader> indexReaderDecorator = new DefaultIndexReaderDecorator();

		File queryFile = new File(topConf.getString("perf.query.file"));
		if (!queryFile.exists()) {
			throw new ConfigurationException(queryFile.getAbsolutePath()+" does not exist!");
		}
		
		
		ZoieSystem<IndexReader, String> zoieSystem = new ZoieSystem<IndexReader, String>(
				dirMgr, interpreter, indexReaderDecorator, zoieConfig);

		SearchQueryHandler queryHandler = new SearchQueryHandler(queryFile, (IndexReaderFactory) zoieSystem);
		
		return new PerfTestHandler(
				(LifeCycleCotrolledDataConsumer<String>) zoieSystem,queryHandler);
	}

	static PerfTestHandler buildNrtHandler(File idxDir, Configuration topConf,Configuration conf)
			throws Exception {
		long throttle = conf.getLong("throttle");
		String modeConf = topConf.getString("perf.directory.type", "file");
		Directory dir;
		if ("file".equals(modeConf)){
			dir = FSDirectory.open(idxDir);
		}
		else if ("mmap".equals(modeConf)){
			dir = MMapDirectory.open(idxDir);
		}
		else if ("nio".equals(modeConf)){
			dir = NIOFSDirectory.open(idxDir);
		}
		else{
			dir = FSDirectory.open(idxDir);
		}
		
		MergePolicy mergePolicy = null;
		
		String mergePolicyConf = conf.getString("mergePolicy");
		if ("tier".equals(mergePolicyConf)){
			mergePolicy = new TieredMergePolicy();
		}
		else if ("zoie".equals(mergePolicyConf)){
			mergePolicy = new ZoieMergePolicy();
		}
		ThrottledLuceneNRTDataConsumer<String> nrtSystem = new ThrottledLuceneNRTDataConsumer<String>(
				dir, new StandardAnalyzer(Version.LUCENE_34),interpreter, throttle,mergePolicy);
		
		boolean appendOnly = conf.getBoolean("appendOnly", false);
		nrtSystem.setAppendOnly(appendOnly);
		
		File queryFile = new File(topConf.getString("perf.query.file"));
		if (!queryFile.exists()) {
			throw new ConfigurationException(queryFile.getAbsolutePath()+" does not exist!");
		}
		
		SearchQueryHandler queryHandler = new SearchQueryHandler(queryFile, (IndexReaderFactory) nrtSystem);
		
		return new PerfTestHandler(
				(LifeCycleCotrolledDataConsumer<String>) nrtSystem,queryHandler);
	}
	
	static PerfTestHandler buildZoieStoreHandler(Configuration topConf,File idxDir, File inputFile) throws Exception {
		String modeConf = topConf.getString("perf.directory.type", "file");
		Directory dir;
		if ("file".equals(modeConf)){
			dir = FSDirectory.open(idxDir);
		}
		else if ("mmap".equals(modeConf)){
			dir = MMapDirectory.open(idxDir);
		}
		else if ("nio".equals(modeConf)){
			dir = NIOFSDirectory.open(idxDir);
		}
		else{
			dir = FSDirectory.open(idxDir);
		}
		ZoieStore luceneStore = LuceneStore.openStore(dir, "src_data", false);
		StoreQueryHandler queryHandler = new StoreQueryHandler(inputFile,luceneStore,100000);
		
		ZoieStoreConsumer<String> consumer = new ZoieStoreConsumer<String>(luceneStore,new ZoieStoreSerializer<String>() {

			@Override
			public long getUid(String data) {
				try{
				  JSONObject obj = new JSONObject(data);
				  return obj.getLong("id");
				}
				catch(Exception e){
					throw new RuntimeException(e);
				}
			}

			@Override
			public byte[] toBytes(String data) {
				return data.getBytes(UTF8);
			}

			@Override
			public String fromBytes(byte[] data) {
				return new String(data,UTF8);
			}

			@Override
			public boolean isDelete(String data) {
				return false;
			}

			@Override
			public boolean isSkip(String data) {
				return false;
			}
		});
		return new PerfTestHandler(consumer,queryHandler);
	}
	
	static PerfTestHandler buildFeedOnlyHandler() throws Exception {
		return new PerfTestHandler(new LifeCycleCotrolledDataConsumer<String>(){

			private volatile String version=null;
			@Override
			public void consume(
					Collection<proj.zoie.api.DataConsumer.DataEvent<String>> data)
					throws ZoieException {
				for (DataEvent<String> datum : data){
					version = datum.getVersion();
				}
			}

			@Override
			public String getVersion() {
				return version;
			}

      @Override
      public Comparator<String> getVersionComparator()
      {
        throw new UnsupportedOperationException("not supported");
      }

			@Override
			public void start() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void stop() {
				// TODO Auto-generated method stub
				
			}
			
		},new QueryHandler<Object>() {

			@Override
			public Object handleQuery() throws Exception {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getCurrentVersion() {
				// TODO Auto-generated method stub
				return null;
			}
		});
	}

	static PerfTestHandler buildPerfHandler(Configuration conf,File inputFile)
			throws Exception {
		File idxDir = new File(conf.getString("perf.idxDir"));

		String type = conf.getString("perf.type");
		Configuration subConf = conf.subset("perf." + type);

		if ("zoie".equals(type)) {
			return buildZoieHandler(idxDir,conf, subConf);
		} else if ("nrt".equals(type)) {
			return buildNrtHandler(idxDir, conf,subConf);
		} else if ("store".equals(type)){
			return buildZoieStoreHandler(conf,idxDir, inputFile);
		}
		else if ("feed".equals(type)){
			return buildFeedOnlyHandler();
		}
		else {
			throw new ConfigurationException("test type: " + type
					+ " is not supported");
		}
	}

	
	public static void runPerf(Configuration conf) throws Exception{
        Map<String,Metric> monitoredMetrics = new HashMap<String,Metric>();
		
		File queryFile = new File(conf.getString("perf.query.file"));
		if (!queryFile.exists()) {
			System.out.println("query file does not exist");
		}
		
		File inputFile = new File(conf.getString("perf.input"));

		if (!inputFile.exists()) {
			throw new ConfigurationException("input file: "
					+ inputFile.getAbsolutePath() + " does not exist.");
		}


		final PerfTestHandler testHandler = buildPerfHandler(conf,inputFile);


		boolean doSearchTest = conf.getBoolean("perf.test.search", true);
		
		
		final TimerMetric searchTimer = Metrics.newTimer(ZoiePerf.class,
				"searchTimer", TimeUnit.NANOSECONDS, TimeUnit.SECONDS);
		final MeterMetric errorMeter = Metrics.newMeter(ZoiePerf.class,
				"errorMeter", "error", TimeUnit.SECONDS);
		
		monitoredMetrics.put("searchTimer", searchTimer);
		monitoredMetrics.put("errorMeter", errorMeter);

		final long waitTime = conf.getLong("perf.query.threadWait", 200);

		final class SearchThread extends Thread {

			private volatile boolean stop = false;

			public void terminate() {
				stop = true;
				synchronized(this){
				  this.notifyAll();
				}
			}

			public void run() {
				while (!stop) {
					try {
						searchTimer.time(new Callable(){
							@Override
							public Object call() throws Exception {
								return testHandler.queryHandler.handleQuery();
							}
						});
						
					} catch (Exception e) {
						errorMeter.mark();
					}
					
					synchronized(this){
						try {
							this.wait(waitTime);
						} catch (InterruptedException e) {
							continue;
						}
					}
				}
			}
		}

		long maxSize = conf.getLong("perf.maxSize");
		
		int feedBatchSize = conf.getInt("perf.feed.batchsize",100);

		final LinedFileDataProvider dataProvider = new LinedFileDataProvider(
				inputFile, 0L);
		
		dataProvider.setBatchSize(feedBatchSize);
		
		dataProvider.setDataConsumer(testHandler.consumer);

		testHandler.consumer.start();

		long dataAmount;
		final long start = System.currentTimeMillis();

		Metric metric = null;
		String name = "eventCount";
		metric = Metrics.newGauge(ZoiePerf.class, name, new GaugeMetric<Long>() {

			@Override
			public Long value() {
				return dataProvider.getEventCount();
			}

		});
		
		monitoredMetrics.put(name, metric);

		name = "amountConsumed";
		metric = Metrics.newGauge(ZoiePerf.class, name,
				new GaugeMetric<Long>() {

					@Override
					public Long value() {
						ZoiePerfVersion ver = ZoiePerfVersion.fromString(testHandler.consumer.getVersion());
						return ver.offsetVersion;
					}

				});


		monitoredMetrics.put(name, metric);
		

		name = "consumeRateCount";
		metric = Metrics.newGauge(ZoiePerf.class, name,
				new GaugeMetric<Long>() {
					@Override
					public Long value() {
						long newTime = System.currentTimeMillis();
						
						long newCount =  dataProvider.getEventCount();
						long timeDelta = newTime - start;
						long countDelta = newCount;

						if (timeDelta == 0)
							return 0L;
						return countDelta * 1000 / timeDelta;
					}

				});


		monitoredMetrics.put(name, metric);
		

		name = "consumeRateMB";
		metric = Metrics.newGauge(ZoiePerf.class, name,
				new GaugeMetric<Long>() {
					@Override
					public Long value() {
						long newTime = System.currentTimeMillis();
						ZoiePerfVersion ver = ZoiePerfVersion.fromString(testHandler.consumer.getVersion());
						long newMB = ver.offsetVersion;

						long timeDelta = newTime - start;
						long mbdelta = newMB;

						if (timeDelta == 0)
							return 0L;
						return mbdelta * 1000 / timeDelta;
					}

				});

		monitoredMetrics.put(name, metric);
		
		name = "indexLatency";
		metric = Metrics.newGauge(ZoiePerf.class, name,
				new GaugeMetric<Long>() {

					@Override
					public Long value() {
						long newCount =  dataProvider.getEventCount();

						String currentReaderVersion = testHandler.queryHandler.getCurrentVersion();
						long readerMarker = ZoiePerfVersion.fromString(currentReaderVersion).countVersion;
						
						long countsBehind = newCount - readerMarker;

						System.out.println("reader marker: "+readerMarker);
						System.out.println("new count: "+newCount);
						return countsBehind;
						
						
					}

				});


		monitoredMetrics.put(name, metric);
		//ConsoleReporter consoleReporter = new ConsoleReporter(System.out);
		//consoleReporter.start(5, TimeUnit.SECONDS);

		//JmxReporter jmxReporter = new JmxReporter(Metrics.defaultRegistry());
		
		File csvOut = new File("csvout");
		csvOut.mkdirs();
		CsvReporter csvReporter = new CsvReporter(csvOut,Metrics.defaultRegistry());
		//GangliaReporter csvReporter = new GangliaReporter(Metrics.defaultRegistry(),"localhost",8649,"zoie-perf");
		
		int updateInterval = conf.getInt("perf.update.intervalSec", 2);
		csvReporter.start(updateInterval, TimeUnit.SECONDS);

		long maxEventsPerMin = conf.getLong("perf.maxEventsPerMin");

		dataProvider.setMaxEventsPerMinute(maxEventsPerMin);

		int numThreads = conf.getInt("perf.query.threads", 10);
		
		SearchThread[] searchThreads = null;
		
		
		if (doSearchTest){
		  searchThreads = new SearchThread[numThreads];
		  for (int i=0;i<numThreads;++i){
			searchThreads[i] = new SearchThread();
		  }
		}
		else{
		  searchThreads = new SearchThread[0];
		}
		
		dataProvider.start();
		
		for (int i=0;i<searchThreads.length;++i){
			searchThreads[i].start();
		}

		ZoiePerfVersion perfVersion = ZoiePerfVersion.fromString(testHandler.consumer.getVersion());
		while ((dataAmount = perfVersion.offsetVersion) < maxSize) {
			Thread.sleep(500);
			perfVersion = ZoiePerfVersion.fromString(testHandler.consumer.getVersion());
		}

		dataProvider.stop();
		testHandler.consumer.stop();

		long end = System.currentTimeMillis();
		
		for (int i=0;i<searchThreads.length;++i){
			searchThreads[i].terminate();
		}
		
		for (int i=0;i<searchThreads.length;++i){
			searchThreads[i].join();
		}

		//consoleReporter.shutdown();
		//jmxReporter.shutdown();
		
		csvReporter.shutdown();

		System.out.println("Test duration: " + (end - start) + " ms");

		System.out.println("Amount of data consumed: " + dataAmount);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		File confFile;
		try {
			confFile = new File(new File(args[0]), "perf.properties");
		} catch (Exception e) {
			confFile = new File(new File("conf"), "perf.properties");
		}

		if (!confFile.exists()) {
			throw new ConfigurationException("configuration file: "
					+ confFile.getAbsolutePath() + " does not exist.");
		}
		

		Configuration conf = new PropertiesConfiguration();
		((PropertiesConfiguration) conf).setDelimiterParsingDisabled(true);
		((PropertiesConfiguration) conf).load(confFile);


		
		Server server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(18888);
        server.addConnector(connector);
 
        ResourceHandler resourceHandler = new ResourceHandler();
        resourceHandler.setWelcomeFiles(new String[]{ "index.html" });
 
        resourceHandler.setResourceBase("./");
        
        ResourceHandler csvDataHandler = new ResourceHandler();
        csvDataHandler.setResourceBase("./csvOut");
        

        server.setHandler(resourceHandler);
        
        final Context context = new Context(server, "/servlets", Context.ALL);
        context.addServlet(new ServletHolder(new ZoiePerfServlet(new File("csvout"))), "/zoie-perf/*");
        
        server.start();
        
        runPerf(conf);
		
        server.join();
        
	}
}
