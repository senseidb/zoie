package proj.zoie.solr;

import java.io.File;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

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

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DirectoryManager.DIRECTORY_MODE;
import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.hourglass.impl.HourGlassScheduler;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.hourglass.mbean.HourglassAdmin;
import proj.zoie.hourglass.mbean.HourglassAdminMBean;
import proj.zoie.impl.indexing.DefaultIndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.mbean.ZoieSystemAdmin;
import proj.zoie.mbean.ZoieSystemAdminMBean;

public class ZoieSystemHome {
  private static Logger log = Logger.getLogger(ZoieSystemHome.class);

  private static final String ZOIE_TYPE_DEFAULT = "zoie";
  private static final String ZOIE_TYPE_HOURGLASS = "hourglass";

  private Zoie<IndexReader, DocumentWithID> _zoieSystem;

  private ZoieSystemHome(SolrCore core) {
    String idxDir = core.getIndexDir();
    File idxFile = new File(idxDir);
    Analyzer analyzer = null;

    try {
      analyzer = core.getSchema().getAnalyzer();
    } catch (Exception e) {
      log.error(e.getMessage() + ", defaulting to " + StandardAnalyzer.class, e);
      analyzer = new StandardAnalyzer(Version.LUCENE_29);
    }

    Similarity similarity = null;
    try {
      similarity = core.getSchema().getSimilarity();
    } catch (Exception e) {
      log.error(e.getMessage() + ", defaulting to " + DefaultSimilarity.class, e);
      similarity = new DefaultSimilarity();
    }

    SolrConfig config = core.getSolrConfig();

    int batchSize = config.getInt("zoie.batchSize", 1000);
    long batchDelay = config.getInt("zoie.batchDelay", 300000);
    boolean realtime = config.getBool("zoie.realtime", true);
    int freshness = config.getInt("zoie.freshness", 10000);
    String type = config.get("zoie.type", ZOIE_TYPE_DEFAULT);
    boolean isTypeZoie;

    if (ZOIE_TYPE_DEFAULT.equals(type)) {
      isTypeZoie = true;
    } else if (ZOIE_TYPE_HOURGLASS.equals(type)) {
      isTypeZoie = false;
    } else {
      throw new IllegalArgumentException("Unsuported Zoie type: " + type);
    }

    ZoieConfig zoieConfig = new ZoieConfig();
    zoieConfig.setBatchSize(batchSize);
    zoieConfig.setBatchDelay(batchDelay);
    zoieConfig.setRtIndexing(realtime);
    zoieConfig.setAnalyzer(analyzer);
    zoieConfig.setSimilarity(similarity);
    zoieConfig.setFreshness(freshness);

    log.info("Zoie System loading with: ");
    log.info("zoie.batchSize: " + batchSize);
    log.info("zoie.batchDelay: " + batchDelay);
    log.info("zoie.realtime: " + realtime);
    log.info("zoie similarity: " + similarity.getClass());
    log.info("zoie analyzer: " + analyzer.getClass());

    DIRECTORY_MODE dirMode;
    String modeValue = config.get("zoie.directory.mode", "SIMPLE");
    if ("SIMPLE".equals(modeValue)) {
      dirMode = DIRECTORY_MODE.SIMPLE;
    } else if ("NIO".equals(modeValue)) {
      dirMode = DIRECTORY_MODE.NIO;
    } else if ("MMAP".equals(modeValue)) {
      dirMode = DIRECTORY_MODE.MMAP;
    } else {
      log.error("directory mode " + modeValue + " is not supported, SIMPLE is used.");
      dirMode = DIRECTORY_MODE.SIMPLE;
    }

    StandardMBean mbean = null;

    if (isTypeZoie) {
      DirectoryManager dirMgr = new DefaultDirectoryManager(idxFile, dirMode);
      ZoieSystem<IndexReader, DocumentWithID> zoieSystem = new ZoieSystem<IndexReader, DocumentWithID>(
          dirMgr, new ZoieSolrIndexableInterpreter(), new DefaultIndexReaderDecorator(), zoieConfig);
      try {
        mbean = new StandardMBean(zoieSystem.getAdminMBean(), ZoieSystemAdminMBean.class);
      } catch (NotCompliantMBeanException e) {
        log.error(e.getMessage(), e);
      }
      _zoieSystem = zoieSystem;
    } else {
      String schedule = config.get("zoie.hourglass.schedule", "00 00 00");
      String freqString = config.get("zoie.hourglass.freq", "day");
      int trimThreshold = config.getInt("zoie.hourglass.trimThreshold", 7);

      FREQUENCY freq;
      if ("day".equals(freqString)) {
        freq = FREQUENCY.DAILY;
      } else if ("min".equals(freqString)) {
        freq = FREQUENCY.MINUTELY;
      } else if ("hour".equals(freqString)) {
        freq = FREQUENCY.HOURLY;
      } else {
        throw new IllegalArgumentException("Unsupported frequency: " + freqString);
      }
      HourGlassScheduler scheduler = new HourGlassScheduler(freq, schedule, true, trimThreshold);
      HourglassDirectoryManagerFactory dirMgrFactory = new HourglassDirectoryManagerFactory(
          idxFile, scheduler, dirMode);
      Hourglass<IndexReader, DocumentWithID> zoieSystem = new Hourglass<IndexReader, DocumentWithID>(
          dirMgrFactory, new ZoieSolrIndexableInterpreter(), new DefaultIndexReaderDecorator(),
          zoieConfig);

      try {
        mbean = new StandardMBean(new HourglassAdmin(zoieSystem), HourglassAdminMBean.class);
      } catch (NotCompliantMBeanException e) {
        log.error(e.getMessage(), e);
      }

      _zoieSystem = zoieSystem;
      log.info("Hourglass: true");
      log.info("hourglass.schedule: " + schedule);
      log.info("hourglass.freqency: " + freq);
      log.info("hourglass.trimeThreshold: " + trimThreshold);
    }

    _zoieSystem.start();

    log.info("Zoie System started ... ");

    IndexReaderFactory readerFactory = core.getIndexReaderFactory();
    if (readerFactory != null && readerFactory instanceof ZoieSolrIndexReaderFactory) {
      ((ZoieSolrIndexReaderFactory) readerFactory).setZoieSystem(_zoieSystem);
    }

    if (mbean != null) {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      try {
        mbeanServer.registerMBean(mbean, new ObjectName("zoie-solr:name=zoie-system"));
      } catch (Exception e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  public Zoie<IndexReader, DocumentWithID> getZoieSystem() {
    return _zoieSystem;
  }

  public void shutdown() {
    try {
      _zoieSystem.flushEvents(100000);
    } catch (ZoieException ze) {
      log.error(ze.getMessage(), ze);
    } finally {
      _zoieSystem.shutdown();
    }
  }

  protected void finalize() {
    shutdown();
  }

  private static ZoieSystemHome instance = null;

  public static ZoieSystemHome getInstance(SolrCore core) {
    if (instance == null) {
      synchronized (ZoieSystemHome.class) {
        if (instance == null) {
          instance = new ZoieSystemHome(core);
        }
      }
    }
    return instance;
  }
}
