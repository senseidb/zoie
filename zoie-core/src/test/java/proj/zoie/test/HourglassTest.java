/**
 *
 */
package proj.zoie.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.jmx.HierarchyDynamicMBean;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.Test;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.hourglass.api.HourglassIndexable;
import proj.zoie.hourglass.api.HourglassIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.hourglass.mbean.HourglassAdmin;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class HourglassTest extends ZoieTestCaseBase {
  static Logger log = Logger.getLogger(HourglassTest.class);

  // Sleep time between each data event for trimming test (in
  // milliseconds)
  int minDirs = Integer.MAX_VALUE; // Minimum number of dirs after system is stable
  int maxDirs = 0;

  @Test
  public void testHourglassDirectoryManagerFactory() throws IOException, InterruptedException,
      ZoieException {
    File idxDir = getIdxDir();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();
    try {
      mbeanServer.registerMBean(hdm, new ObjectName("HouseGlass:name=log4j"));
      // Add the root logger to the Hierarchy MBean
      hdm.addLoggerMBean(Logger.getRootLogger().getName());

      // Get each logger from the Log4J Repository and add it to
      // the Hierarchy MBean created above.
      LoggerRepository r = LogManager.getLoggerRepository();

      java.util.Enumeration loggers = r.getCurrentLoggers();

      int count = 1;
      while (loggers.hasMoreElements()) {
        String name = ((Logger) loggers.nextElement()).getName();
        if (log.isDebugEnabled()) {
          log.debug("[contextInitialized]: Registering " + name);
        }
        hdm.addLoggerMBean(name);
        count++;
      }
      if (log.isInfoEnabled()) {
        log.info("[contextInitialized]: " + count + " log4j MBeans registered.");
      }
    } catch (Exception e) {
      log.error("[contextInitialized]: Exception catched: ", e);
    }
    String schedule = "07 15 20";
    long numTestContent = 10250;
    oneTest(idxDir, schedule, numTestContent, true); // test starting from empty index
    oneTest(idxDir, schedule, numTestContent, true); // test index pick up
    oneTest(idxDir, schedule, numTestContent, false); // test index pick up

    modifyTest(idxDir, schedule); // test deletes and updates
    return;
  }

  @Test
  public void testTrimming() throws Exception {
    File idxDir = getIdxDir();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();
    try {
      mbeanServer.registerMBean(hdm, new ObjectName("HouseGlass:name=log4j"));
      // Add the root logger to the Hierarchy MBean
      hdm.addLoggerMBean(Logger.getRootLogger().getName());

      // Get each logger from the Log4J Repository and add it to
      // the Hierarchy MBean created above.
      LoggerRepository r = LogManager.getLoggerRepository();

      java.util.Enumeration loggers = r.getCurrentLoggers();

      int count = 1;
      while (loggers.hasMoreElements()) {
        String name = ((Logger) loggers.nextElement()).getName();
        if (log.isDebugEnabled()) {
          log.debug("[contextInitialized]: Registering " + name);
        }
        hdm.addLoggerMBean(name);
        count++;
      }
      if (log.isInfoEnabled()) {
        log.info("[contextInitialized]: " + count + " log4j MBeans registered.");
      }
    } catch (Exception e) {
      log.error("[contextInitialized]: Exception catched: ", e);
    }
    String schedule = "07 15 20";

    System.out.println("Testing trimming, please wait for about 4 mins...");

    int trimThreshold = 1;
    doTrimmingTest(idxDir, schedule, trimThreshold);

    assertTrue("Maxdir should be >= than " + (trimThreshold + 2) + "but it's " + maxDirs,
      maxDirs >= trimThreshold + 2);
    assertEquals(trimThreshold + 1, minDirs);
    return;
  }

  private void modifyTest(File idxDir, String schedule) throws IOException, InterruptedException {
    HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(idxDir,
        new HourGlassScheduler(HourGlassScheduler.FREQUENCY.MINUTELY, schedule, false, 100));
    ZoieConfig zConfig = new ZoieConfig();
    zConfig.setBatchSize(100000);
    zConfig.setMaxBatchSize(100000);
    zConfig.setBatchDelay(30000);
    zConfig.setFreshness(100);
    zConfig.setRtIndexing(true);
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory,
        new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>() {

          @Override
          public IndexReader decorate(ZoieSegmentReader<IndexReader> indexReader)
              throws IOException {
            return indexReader;
          }

          @Override
          public IndexReader redecorate(IndexReader decorated, ZoieSegmentReader<IndexReader> copy)
              throws IOException {
            return decorated;
          }

          @Override
          public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
            // do nothing
          }
        }, zConfig);
    HourglassAdmin mbean = new HourglassAdmin(hourglass);
    // HourglassAdminMBean mbean = admin.getMBean();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.registerMBean(mbean, new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      System.out.println(e);
    }
    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setBatchSize(800);
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();

    Thread.sleep(200); // wait freshness time for zoie readers.

    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);
    assertTrue("initNumDocs should > 2", initNumDocs > 2);

    List<ZoieMultiReader<IndexReader>> readers = hourglass.getIndexReaders();
    try {
      assertTrue("before delete, 0 should be found", findUID(readers, 0));
      assertTrue("before update, 1 should be found", findUID(readers, 1));
    } finally {
      hourglass.returnIndexReaders(readers);
    }

    List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(3);
    // Delete uid 0
    list.add(new DataEvent<String>("D" + 0, "" + initNumDocs));
    // Update uid 1
    list.add(new DataEvent<String>("U" + 1, "" + (initNumDocs + 1)));
    // The last one
    int lastUID = 100000;
    list.add(new DataEvent<String>("U" + lastUID, "" + (initNumDocs + 2)));
    memoryProvider.addEvents(list);
    memoryProvider.flush();

    while (true) {
      readers = hourglass.getIndexReaders();
      try {
        if (findUID(readers, lastUID)) break;
      } finally {
        hourglass.returnIndexReaders(readers);
      }
      Thread.sleep(100);
    }

    readers = hourglass.getIndexReaders();
    try {
      assertTrue("0 is deleted, should not be found", !findUID(readers, 0));
      assertTrue("1 is updated, should be found", findUID(readers, 1));
    } finally {
      hourglass.returnIndexReaders(readers);
    }

    try {
      mbeanServer.unregisterMBean(new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e);
    }
    memoryProvider.stop();
    hourglass.shutdown();
  }

  private boolean findUID(List<ZoieMultiReader<IndexReader>> readers, long uid) {
    boolean found = false;
    for (ZoieMultiReader<IndexReader> reader : readers) {
      int doc = reader.getDocIDMaper().getDocID(uid);
      if (doc != DocIDMapper.NOT_FOUND && !reader.isDeleted(doc)) {
        found = true;
        if (reader.directory() instanceof FSDirectory) System.out.println("Found uid: " + uid
            + " at: " + ((FSDirectory) reader.directory()).getDirectory());
        else System.out.println("Found uid: " + uid + " at RAMIndexFactory");
        break;
      }
    }
    return found;
  }

  private void oneTest(File idxDir, String schedule, long numTestContent, boolean appendOnly)
      throws IOException, InterruptedException {
    HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(idxDir,
        new HourGlassScheduler(HourGlassScheduler.FREQUENCY.MINUTELY, schedule, appendOnly, 100));
    ZoieConfig zConfig = new ZoieConfig();
    zConfig.setBatchSize(100000);
    zConfig.setMaxBatchSize(100000);
    zConfig.setBatchDelay(30000);
    zConfig.setFreshness(100);
    zConfig.setRtIndexing(true);
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory,
        new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>() {

          @Override
          public IndexReader decorate(ZoieSegmentReader<IndexReader> indexReader)
              throws IOException {
            return indexReader;
          }

          @Override
          public IndexReader redecorate(IndexReader decorated, ZoieSegmentReader<IndexReader> copy)
              throws IOException {
            return decorated;
          }

          @Override
          public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
            // do nothing
          }
        }, zConfig);
    HourglassAdmin mbean = new HourglassAdmin(hourglass);
    // HourglassAdminMBean mbean = admin.getMBean();
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.registerMBean(mbean, new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      System.out.println(e);
    }
    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setBatchSize(800);
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();

    Thread.sleep(200); // wait freshness time for zoie readers.

    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);

    for (int i = initNumDocs; i < initNumDocs + numTestContent; i++) {
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(2);
      list.add(new DataEvent<String>("" + i, "" + i));
      memoryProvider.addEvents(list);

      if (i == 0 || i % 1130 != 0) continue;
      memoryProvider.flush();
      int numDoc = -1;
      List<ZoieMultiReader<IndexReader>> readers = null;
      IndexReader reader = null;
      IndexSearcher searcher = null;
      while (numDoc < i + 1) {
        if (reader != null && readers != null) {
          searcher = null;
          reader.close();
          hourglass.returnIndexReaders(readers);
        }
        readers = hourglass.getIndexReaders();
        reader = new MultiReader(readers.toArray(new IndexReader[0]), false);
        searcher = new IndexSearcher(reader);
        TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
        numDoc = hitsall.totalHits;
        Thread.sleep(100);
      }
      TopDocs hits = searcher.search(new TermQuery(new Term("contents", "" + i)), 10);
      TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
      try {
        assertEquals("one hit for " + i, 1, hits.totalHits);
        assertEquals("MatchAllDocsHit ", i + 1, hitsall.totalHits);
      } finally {
        searcher = null;
        reader.close();
        reader = null;
        hourglass.returnIndexReaders(readers);
        readers = null;
      }
      System.out.println(((i - initNumDocs) * 100 / numTestContent) + "%");
    }
    try {
      mbeanServer.unregisterMBean(new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e);
    }
    memoryProvider.stop();
    hourglass.shutdown();
  }

  public static String now() {
    Calendar cal = Calendar.getInstance();
    SimpleDateFormat sdf = new SimpleDateFormat("hh:mm:ss aaa");
    return sdf.format(cal.getTime());

  }

  private void doTrimmingTest(File idxDir, String schedule, int trimThreshold) throws Exception {
    HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(
        idxDir,
        new HourGlassScheduler(HourGlassScheduler.FREQUENCY.MINUTELY, schedule, true, trimThreshold) {
          volatile Long nextTime;

          @Override
          protected Calendar getNextRoll() {
            if (nextTime == null) {
              nextTime = System.currentTimeMillis();
            }
            nextTime += 1000;
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(nextTime);
            return calendar;
          }

          @Override
          public Calendar getTrimTime(Calendar now) {
            Calendar ret = Calendar.getInstance();
            ret.setTimeInMillis(now.getTimeInMillis() + 60L * 60 * 1000 * 48);
            return ret;
          }
        });
    ZoieConfig zConfig = new ZoieConfig();
    zConfig.setBatchSize(1);
    zConfig.setBatchDelay(10);
    zConfig.setFreshness(10);
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory,
        new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>() {

          @Override
          public IndexReader decorate(ZoieSegmentReader<IndexReader> indexReader)
              throws IOException {
            return indexReader;
          }

          @Override
          public IndexReader redecorate(IndexReader decorated, ZoieSegmentReader<IndexReader> copy)
              throws IOException {
            return decorated;
          }

          @Override
          public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
            // Do nothing
          }
        }, zConfig);
    HourglassAdmin mbean = new HourglassAdmin(hourglass);
    Object readerManager = getFieldValue(hourglass, "_readerMgr");
    Object maintenanceThread = getFieldValue(readerManager, "maintenanceThread");
    Runnable runnable = (Runnable) getFieldValue(maintenanceThread, "target");
    ZoieSystem currentZoie = (ZoieSystem) getFieldValue(hourglass, "_currentZoie");
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    try {
      mbeanServer.registerMBean(mbean, new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      System.out.println(e);
    }
    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();

    Thread.sleep(200); // wait freshness time for zoie readers.

    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);
    boolean wait = false;
    for (int i = initNumDocs; i < initNumDocs + 1200; i++) {
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(2);
      list.add(new DataEvent<String>("" + i, "" + i));
      memoryProvider.addEvents(list);
      System.out.println((i - initNumDocs + 1) + " of " + (1200 - initNumDocs));
      if (idxDir.exists()) {
        int numDirs = idxDir.listFiles().length;
        if (numDirs > maxDirs) {
          System.out.println("Set maxDirs to " + numDirs);
          maxDirs = numDirs;
        }
        if (maxDirs >= trimThreshold + 2) {
          wait = true;
          if (numDirs < minDirs) {
            boolean stop = false;

            // We want to make sure that number of directories does shrink
            // to trimThreshold + 1. Exactly when trimming is done is
            // controlled by HourglassReaderManager, which checks trimming
            // condition only once per minute.
            System.out.println("Set minDirs to " + numDirs);
            minDirs = numDirs;
            if (minDirs == 2) {
              stop = true;
            }
            if (stop) {
              System.out.println("TrimmingTest succeeded, terminate the loop.");
              break;
            }
          }
        }
      }
      synchronized (currentZoie) {
        currentZoie.notifyAll();
      }
      if (wait) {
        Thread.sleep(1000);
      }
      synchronized (runnable) {
        runnable.notifyAll();
      }
      synchronized (currentZoie) {
        Thread.sleep(10);
      }

    }
    Thread.sleep(500);
    synchronized (runnable) {
      runnable.notifyAll();
    }

    try {
      mbeanServer.unregisterMBean(new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e);
    }
    memoryProvider.stop();
    hourglass.shutdown();
  }

  private Object getFieldValue(Object obj, String fieldName) {
    try {
      java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);

      return field.get(obj);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private int getTotalNumDocs(Hourglass<IndexReader, String> hourglass) {
    int numDocs = 0;
    List<ZoieMultiReader<IndexReader>> readers = null;
    try {
      readers = hourglass.getIndexReaders();
      for (ZoieMultiReader<IndexReader> reader : readers) {
        numDocs += reader.numDocs();
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (readers != null) hourglass.returnIndexReaders(readers);
    }
    return numDocs;
  }

  public static class TestHourglassIndexable extends HourglassIndexable {
    protected static long nextUID = 0;
    public final long UID;
    final String _str;

    public TestHourglassIndexable(String str) {
      if (str.charAt(0) < '0' || str.charAt(0) > '9') UID = Long.parseLong(str.substring(1));
      else UID = getNextUID();
      _str = str;
    }

    public static final synchronized long getNextUID() {
      return nextUID++;
    }

    @Override
    public final long getUID() {
      return UID;
    }

    public Document buildDocument() {
      Document doc = new Document();
      doc.add(new TextField("contents", _str, Store.YES));
      return doc;
    }

    @Override
    public IndexingReq[] buildIndexingReqs() {
      return new IndexingReq[] { new IndexingReq(buildDocument(), null) };
    }

    @Override
    public boolean isSkip() {
      return false;
    }

    @Override
    public final boolean isDeleted() {
      return _str.charAt(0) == 'D';
    }
  }

  public static class HourglassTestInterpreter implements HourglassIndexableInterpreter<String> {

    @Override
    public HourglassIndexable convertAndInterpret(String src) {
      if (log.isDebugEnabled()) {
        log.debug("converting " + src);
      }
      return new TestHourglassIndexable(src);
    }

  }
}
