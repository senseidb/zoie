package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Comparator;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.junit.After;
import org.junit.Before;

import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.SimpleReaderCache;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.DataInterpreterForTests;

public class ZoieTestCaseBase {
  static {
    Appender appender = null;
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%d{yyyy/MM/dd HH:mm:ss.SSS} %p [%c{1}] [%x] %m%n");
    try {
      appender = new org.apache.log4j.FileAppender(layout, "zoie-test-log.log");
    } catch (IOException e) {
      e.printStackTrace();
    }
    org.apache.log4j.BasicConfigurator.configure(appender);
  }
  static Logger log = Logger.getLogger(ZoieTestCaseBase.class);
  static MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

  public void registerMBean(Object standardmbean, String mbeanname) {
    try {
      mbeanServer.registerMBean(standardmbean, new ObjectName("Zoie:name=" + mbeanname));
    } catch (Exception e) {
      log.warn(e);
    }
  }

  public void unregisterMBean(String mbeanname) {
    try {
      mbeanServer.unregisterMBean(new ObjectName("Zoie:name=" + mbeanname));
    } catch (Exception e) {
      log.warn(e);
    }
  }

  @After
  @Before
  public void tearDown() {
    deleteDirectory(getIdxDir());
  }

  protected static File getIdxDir() {
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    File tempFile = new File(tmpDir, "test-idx");
    int i = 0;
    while (tempFile.exists()) {
      if (i > 10) {
        log.info("cannot delete");
        throw new IllegalStateException("cannot delete");

      }
      log.info("deleting " + tempFile);
      deleteDirectory(tempFile);
      // tempFile.delete();
      try {
        Thread.sleep(50);
      } catch (Exception e) {
        log.error("thread interrupted in sleep in deleting file" + e);
      }
      i++;
    }
    return tempFile;
  }

  protected static File getTmpDir() {
    return new File(System.getProperty("java.io.tmpdir"));
  }

  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      Comparator<String> versionComparator) {
    return createZoie(idxDir, realtime, 20, versionComparator);
  }

  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      Comparator<String> versionComparator, boolean immediateRefresh) {
    return createZoie(idxDir, realtime, 20, versionComparator, immediateRefresh);
  }

  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      long delay, Comparator<String> versionComparator, boolean immediateRefresh) {
    return createZoie(idxDir, realtime, delay, null, null, versionComparator, immediateRefresh);
  }

  /**
   * @param idxDir
   * @param realtime
   * @param delay delay for interpreter (simulating a slow interpreter)
   * @param zoieVersionFactory
   * @return
   */
  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      long delay, Comparator<String> versionComparator) {
    return createZoie(idxDir, realtime, delay, null, null, versionComparator, false);
  }

  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      DocIDMapperFactory docidMapperFactory, Comparator<String> versionComparator) {
    return createZoie(idxDir, realtime, 2, null, docidMapperFactory, versionComparator, false);
  }

  /**
   * @param idxDir
   * @param realtime
   * @param delay delay for interpreter (simulating a slow interpreter)
   * @param analyzer
   * @param docidMapperFactory
   * @param zoieVersionFactory
   * @return
   */
  protected static ZoieSystem<IndexReader, String> createZoie(File idxDir, boolean realtime,
      long delay, Analyzer analyzer, DocIDMapperFactory docidMapperFactory,
      Comparator<String> versionComparator, boolean immediateRefresh) {
    ZoieConfig config = new ZoieConfig();
    config.setDocidMapperFactory(docidMapperFactory);
    config.setBatchSize(50);
    config.setBatchDelay(2000);
    config.setRtIndexing(realtime);
    config.setVersionComparator(versionComparator);
    config.setSimilarity(null);
    config.setAnalyzer(null);
    if (immediateRefresh) {
      config.setReadercachefactory(SimpleReaderCache.FACTORY);
    }
    ZoieSystem<IndexReader, String> idxSystem = new ZoieSystem<IndexReader, String>(idxDir,
        new DataInterpreterForTests(delay, analyzer), new TestIndexReaderDecorator(), config);
    return idxSystem;
  }

  protected static class TestIndexReaderDecorator implements IndexReaderDecorator<IndexReader> {
    @Override
    public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader) throws IOException {
      return indexReader.getInnerReader();
    }

    @Override
    public IndexReader redecorate(IndexReader decorated, ZoieIndexReader<IndexReader> copy) throws IOException {
      return decorated;
    }

    @Override
    public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
      // do nothing
    }
  }

  protected static boolean deleteDirectory(File path) {
    if (path.exists()) {
      File[] files = path.listFiles();
      for (int i = 0; i < files.length; i++) {
        if (files[i].isDirectory()) {
          deleteDirectory(files[i]);
        } else {
          files[i].delete();
        }
      }
    }
    return (path.delete());
  }

  protected static class QueryThread extends Thread {
    public volatile boolean stop = false;
    public volatile boolean mismatch = false;
    public volatile String message = null;
    public Exception exception = null;
  }

}
