package proj.zoie.server;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;


public class ZoieServer {
	private static final Logger log = Logger.getLogger(ZoieServer.class);
	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		String confDirName=System.getProperty("conf.dir");
		File confDir = null;
		if (confDirName == null)
		{
			confDir = new File("conf");
		}
		else
		{
			confDir = new File(confDirName);
		}

		System.out.println("using config dir: "+confDir.getAbsolutePath());
		
		Properties props = new Properties();
		File serverPropFile = new File(confDir,"server.properties");
		if (confDir.exists() && serverPropFile.exists())
		{
			FileInputStream fin =null;
			try
			{
				fin = new FileInputStream(serverPropFile);
				props.load(fin);
			}
			catch(Exception e)
			{
				log.error("propblem loading conf file, using default settings...");
			}
			finally
			{
				if (fin!=null)
				{
					fin.close();
				}
			}
		}
		
		log.info("loaded properties: "+props);
		
		String warDirName = props.getProperty("war.dir");
		if (warDirName == null) throw new IllegalArgumentException("war.dir property not specified");
		File warDir = new File(warDirName);
		
		int minThread;
		try
		{
			minThread=Integer.parseInt(props.getProperty("min.thread"));
		}
		catch(Exception e)
		{
			log.error("defaulting min.thread to 50");
			minThread=50;
		}
		
		int maxThread;
		try
		{
			maxThread=Integer.parseInt(props.getProperty("max.thread"));
		}
		catch(Exception e)
		{
			log.error("defaulting max.thread to 75");
			maxThread=75;
		}
		
		
		int maxIdleTime;
		try
		{
			maxIdleTime=Integer.parseInt(props.getProperty("max.ideltime"));
		}
		catch(Exception e)
		{
			log.error("defaulting max.ideltime to 2000");
			maxIdleTime=2000;
		}
		
		QueuedThreadPool threadPool = new QueuedThreadPool();
		threadPool.setName("server(jetty) threads");
		threadPool.setMinThreads(minThread);
		threadPool.setMaxThreads(maxThread);
		threadPool.setMaxIdleTimeMs(maxIdleTime);
		threadPool.start();
		
		log.info("request threadpool started.");
		
		final Server server = new Server();
		server.setThreadPool(threadPool);
		
		String indexDir = props.getProperty("index.directory");
		if (indexDir!=null)
		{
			System.getProperties().put("index.directory", indexDir);
		}
		
	    SelectChannelConnector connector = new SelectChannelConnector();
	    int serverPort;
	    try
	    {
	    	serverPort = Integer.parseInt(props.getProperty("server.port"));
	    }
	    catch(Exception e)
	    {
	    	log.warn("server port defaulting to 8888");
	    	serverPort=8888;
	    }
	    connector.setPort(serverPort);
	    server.addConnector(connector);

	    File[] warFiles = warDir.listFiles(new FileFilter(){
			public boolean accept(File pathname) {
				if (pathname.isDirectory()) return true;
				String name = pathname.getName();
				return name.endsWith(".war");
			}
	    });
	    
	    log.info("loading wars...");
	    for (File warFile : warFiles)
	    {
	    	try
	    	{
	    	  WebAppContext web = new WebAppContext();
	    	  String fileName=warFile.getName();
	    	  log.info("loading war: "+fileName);
	    	  if (!warFile.isDirectory()){
	    	    int index = fileName.lastIndexOf(".war");
	    	    fileName = fileName.substring(0, index);
	    	    web.setExtractWAR(true);
	    	  }
		      web.setContextPath("/"+fileName);
		      log.info("context path: /"+fileName);
		      web.setWar(warFile.getAbsolutePath());
		      if (indexDir!=null)
		      {
		        web.setAttribute("index.directory", indexDir);
		      }
		      server.addHandler(web);
	    	}
	    	catch(Exception e)
	    	{
	    		log.error("Problem deploying application: "+warFile,e);
	    	}
	    }

	    log.info("finished loading wars.");
	    
	    Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run()
			{
                log.info("shutting down...");
                try {
					server.stop();
				} catch (Exception e) {
					log.error(e.getMessage(),e);
				}
				finally
				{
                  server.destroy();
                  log.info("shutdown successful");
				}
			}
		});

	    try {
		  log.info("starting server ... ");
	      server.start();
		  log.info("server started.");
	    } catch (Exception e) {
	      log.error(e.getMessage(),e);
	    }
	    
	    try
	    {
	    	server.join();
	    }
	    catch(Exception e)
	    {
	    	System.exit(100);
	    }
	}
}
