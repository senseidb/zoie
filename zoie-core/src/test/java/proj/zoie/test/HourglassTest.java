/**
 *
 */
package proj.zoie.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.junit.Test;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
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
	public void testHourglassDirectoryManagerFactory() throws IOException,
			InterruptedException, ZoieException {
		File idxDir = getIdxDir();
		MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
		HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();
		try {
			mbeanServer.registerMBean(hdm, new ObjectName(
					"HouseGlass:name=log4j"));
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
				log.info("[contextInitialized]: " + count
						+ " log4j MBeans registered.");
			}
		} catch (Exception e) {
			log.error("[contextInitialized]: Exception catched: ", e);
		}
		String schedule = "07 15 20";
		long numTestContent = 10250;
		oneTest(idxDir, schedule, numTestContent); // test starting from empty
													// index
		oneTest(idxDir, schedule, numTestContent); // test index pick up
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
		doTrimmingTest(idxDir, schedule,  trimThreshold);

    assertTrue("Maxdir should be > than " + (trimThreshold + 2) + "but it's " + maxDirs,  maxDirs >= trimThreshold + 2);
    assertEquals(trimThreshold + 1, minDirs);
		return;
	}

	private void oneTest(File idxDir, String schedule, long numTestContent)
			throws IOException, InterruptedException {
		HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(
				idxDir, new HourGlassScheduler(
						HourGlassScheduler.FREQUENCY.MINUTELY, schedule, 100));
		ZoieConfig zConfig = new ZoieConfig();
		zConfig.setBatchSize(100000);
		zConfig.setMaxBatchSize(100000);
		zConfig.setBatchDelay(30000);
		zConfig.setFreshness(100);
		zConfig.setRtIndexing(true);
		Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(
				factory, new HourglassTestInterpreter(),
				new IndexReaderDecorator<IndexReader>() {

					@Override
          public IndexReader decorate(
							ZoieIndexReader<IndexReader> indexReader)
							throws IOException {
						return indexReader;
					}

					@Override
          public IndexReader redecorate(IndexReader decorated,
							ZoieIndexReader<IndexReader> copy,
							boolean withDeletes) throws IOException {
						// TODO Auto-generated method stub
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
			mbeanServer.registerMBean(mbean, new ObjectName(
					"HouseGlass:name=hourglass"));
		} catch (Exception e) {
			System.out.println(e);
		}
		MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
		memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
		memoryProvider.setBatchSize(800);
		memoryProvider.setDataConsumer(hourglass);
		memoryProvider.start();

		int initNumDocs = getTotalNumDocs(hourglass);
		System.out.println("initial number of DOCs: " + initNumDocs);

		long accumulatedTime = 0;
		for (int i = initNumDocs; i < initNumDocs + numTestContent; i++) {
			List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(2);
			list.add(new DataEvent<String>("" + i, "" + i));
			memoryProvider.addEvents(list);

			if (i ==0 || i % 1130 != 0)
				continue;
			memoryProvider.flush();
			long flushtime = System.currentTimeMillis();
			int numDoc = -1;
			List<ZoieIndexReader<IndexReader>> readers = null;
			IndexReader reader = null;
			Searcher searcher = null;
			int oldNum = -1;
			while (numDoc < i + 1) {
				if (reader != null && readers != null) {
					searcher.close();
					searcher = null;
					reader.close();
					hourglass.returnIndexReaders(readers);
				}
				readers = hourglass.getIndexReaders();
				reader = new MultiReader(readers.toArray(new IndexReader[0]),
						false);
				searcher = new IndexSearcher(reader);
				TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
				numDoc = hitsall.totalHits;
				oldNum = numDoc;
				Thread.sleep(100);
			}
			accumulatedTime += (System.currentTimeMillis() - flushtime);
			TopDocs hits = searcher.search(new TermQuery(new Term("contents",
					"" + i)), 10);
			TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
			try {
				assertEquals("one hit for " + i, 1, hits.totalHits);
				assertEquals("MatchAllDocsHit ", i + 1, hitsall.totalHits);
			} finally {
				searcher.close();
				searcher = null;
				reader.close();
				reader = null;
				hourglass.returnIndexReaders(readers);
				readers = null;
			}
			System.out
					.println(((i - initNumDocs) * 100 / numTestContent) + "%");
		}
		try {
			mbeanServer.unregisterMBean(new ObjectName(
					"HouseGlass:name=hourglass"));
		} catch (Exception e) {
			e.printStackTrace();
			log.error(e);
		}
		memoryProvider.stop();
		hourglass.shutdown();
	}

  private void doTrimmingTest(File idxDir, String schedule,  int trimThreshold) throws Exception  {
    HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(idxDir, new HourGlassScheduler(
        HourGlassScheduler.FREQUENCY.MINUTELY, schedule, trimThreshold) {
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
    zConfig.setBatchSize(3);
    zConfig.setBatchDelay(10);
    zConfig.setFreshness(10);
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory,
        new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>() {

          @Override
          public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader) throws IOException {
            return indexReader;
          }

          @Override
          public IndexReader redecorate(IndexReader decorated, ZoieIndexReader<IndexReader> copy, boolean withDeletes)
              throws IOException {
            // TODO Auto-generated method stub
            return decorated;
          }

          @Override
          public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
            // Do nothing
          }
        }, zConfig);
    HourglassAdmin mbean = new HourglassAdmin(hourglass);
    java.lang.reflect.Field field;
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
    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);

    for (int i = initNumDocs; i < initNumDocs + 600; i++) {
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(2);
      list.add(new DataEvent<String>("" + i, "" + i));
      memoryProvider.addEvents(list);

      System.out.println((i - initNumDocs + 1) + " of " + (80 - initNumDocs));
      if (idxDir.exists()) {
        int numDirs = idxDir.listFiles().length;
        System.out.println("!!" + numDirs);
        if (numDirs > maxDirs) {
          System.out.println("Set maxDirs to " + numDirs);
          maxDirs = numDirs;
        }
        if (maxDirs >= trimThreshold + 2 && numDirs < minDirs) {
          boolean stop = false;
          if (minDirs < 5) {
            stop = true;
          }
          // We want to make sure that number of directories does shrink
          // to trimThreshold + 1. Exactly when trimming is done is
          // controlled by HourglassReaderManager, which checks trimming
          // condition only once per minute.
          System.out.println("Set minDirs to " + numDirs);
          minDirs = numDirs;
          if (stop) {break;}
        }
      }
      synchronized (runnable) {
        runnable.notifyAll();
      }
      currentZoie.flushEvents(500);
    }
    Thread.sleep(500);
    synchronized (runnable) {
      runnable.notifyAll();
    }


    int numDirs = idxDir.listFiles().length;
    System.out.println("!!!" + numDirs);
    try {
      mbeanServer.unregisterMBean(new ObjectName("HouseGlass:name=hourglass"));
    } catch (Exception e) {
      e.printStackTrace();
      log.error(e);
    }
    memoryProvider.stop();
    hourglass.shutdown();
  }
  private Object getFieldValue(Object obj, String fieldName)  {
    try {
    java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);

    return field.get(obj);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

	private int getTotalNumDocs(
			Hourglass<IndexReader, String> hourglass) {
		int numDocs = 0;
		List<ZoieIndexReader<IndexReader>> readers = null;
		try {
			readers = hourglass.getIndexReaders();
			for (ZoieIndexReader<IndexReader> reader : readers) {
				numDocs += reader.numDocs();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (readers != null)
				hourglass.returnIndexReaders(readers);
		}
		return numDocs;
	}

	public static class TestHourglassIndexable extends
			HourglassIndexable {
		protected static long nextUID = System.currentTimeMillis();
		public final long UID;
		final String _str;

		public TestHourglassIndexable(String str) {
			UID = getNextUID();
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
			doc.add(new Field("contents", _str, Store.YES, Index.ANALYZED));
			/*try {
				Thread.sleep(25); // slow down indexing process
			} catch (Exception e) {
			}*/
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

		/*@Override
		public byte[] getStoreValue() {
			return String.valueOf(getUID()).getBytes();
		}*/
	}

	public static class HourglassTestInterpreter implements
			HourglassIndexableInterpreter<String> {

		@Override
    public HourglassIndexable convertAndInterpret(String src) {
			if (log.isDebugEnabled()){
			  log.debug("converting " + src);
			}
			return new TestHourglassIndexable(src);
		}

	}
}
