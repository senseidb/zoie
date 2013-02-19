package proj.zoie.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.junit.Test;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.DataForTests;

public class ZoieThreadTest extends ZoieTestCaseBase {
	static Logger log = Logger.getLogger(ZoieThreadTest.class);

	public ZoieThreadTest() {
	}

	private static abstract class QueryRunnable implements Runnable {
		public volatile boolean stop = false;
		public volatile boolean mismatch = false;
		public volatile String message = null;
		public Exception exception = null;
	}

	@Test
	public void testThreadDelImpl() throws ZoieException {
		File idxDir = getIdxDir();
		final ZoieSystem<IndexReader, String> idxSystem = createZoie(
				idxDir, true, 100, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
		for (String bname : idxSystem.getStandardMBeanNames()) {
			registerMBean(idxSystem.getStandardMBean(bname), bname);
		}
		idxSystem.start();
		final String query = "zoie";
		int numThreads = 5;
		QueryRunnable[] queryRunnables = new QueryRunnable[numThreads];
		for (int i = 0; i < queryRunnables.length; i++) {
			queryRunnables[i] = new QueryRunnable() {
				@Override
        public void run() {
					QueryParser parser = new QueryParser(
							Version.LUCENE_34, "contents",
							idxSystem.getAnalyzer());
					Query q;
					try {
						q = parser.parse(query);
					} catch (Exception e) {
						exception = e;
						return;
					}

					int expected = DataForTests.testdata.length;
					while (!stop) {
						Searcher searcher = null;
						List<ZoieIndexReader<IndexReader>> readers = null;
						MultiReader reader = null;
						try {
							readers = idxSystem.getIndexReaders();
							reader = new MultiReader(
									readers.toArray(new IndexReader[readers
											.size()]), false);

							searcher = new IndexSearcher(reader);

							TopDocs hits = searcher.search(q, 10);
							int count = hits.totalHits;

							if (count != expected) {
								mismatch = true;
								message = "hit count: " + count
										+ " / expected: " + expected;
								stop = true;
								StringBuffer sb = new StringBuffer();
								sb.append(message + "\n");
								sb.append("each\n");
								sb.append(groupDump(readers, q));
								sb.append("main\n");
								sb.append(dump(reader, hits));
								System.out.println(sb.toString());
								log.info(sb.toString());
							}
							Thread.sleep(20);
						} catch (Exception ex) {
							ex.printStackTrace();
							exception = ex;
							stop = true;
						} finally {
							try {
								if (searcher != null) {
									searcher.close();
									reader.close();
									reader = null;
									searcher = null;
								}
							} catch (IOException ioe) {
								log.error(ioe.getMessage(), ioe);
							} finally {
								idxSystem.returnIndexReaders(readers);
							}
						}
					}
				}

				private String groupDump(
						List<ZoieIndexReader<IndexReader>> readers, Query q)
						throws IOException {
					StringBuffer sb = new StringBuffer();
					for (ZoieIndexReader<IndexReader> reader : readers) {
						sb.append(reader).append("\n");
						Searcher searcher = new IndexSearcher(reader);
						TopDocs hits = searcher.search(q, 20);
						sb.append(dump(reader, hits));
						searcher.close();
						searcher = null;
					}
					return sb.toString();
				}

				private String dump(IndexReader reader, TopDocs hits)
						throws CorruptIndexException, IOException {
					StringBuffer sb = new StringBuffer();
					ScoreDoc[] sd = hits.scoreDocs;
					long[] uids = new long[sd.length];
					try {
						if (reader.hasDeletions())
							sb.append(" there are deletions @ version: "
									+ reader.getVersion());
					} catch (UnsupportedOperationException e) {
						if (reader.hasDeletions())
							sb.append(" there are deletions @ version: N/A");
					}
					sb.append("\n");
					for (int i = 0; i < sd.length; i++) {
						Document doc = reader.document(sd[i].doc);
						uids[i] = Long.parseLong(doc.get("id"));
						if (reader.isDeleted(sd[i].doc)) {
							sb.append(
									"doc: " + sd[i].doc + " with uid: "
											+ uids[i] + " has been deleted")
									.append("\n");
						}
					}
					sb.append(Thread.currentThread() + Arrays.toString(uids))
							.append("\n");
					int max = reader.maxDoc();
					uids = new long[max];
					for (int i = 0; i < max; i++) {
						Document doc = reader.document(i);
						uids[i] = Long.parseLong(doc.get("id"));
						if (reader.isDeleted(i)) {
							sb.append(
									"doc: " + i + " with uid: " + uids[i]
											+ " has been deleted").append("\n");
						}
					}
					sb.append("uids: " + Arrays.toString(uids)).append("\n");
					return sb.toString();
				}
			};
		}

		MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
		memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
		memoryProvider.setDataConsumer(idxSystem);
		memoryProvider.start();
		ExecutorService threadPool = Executors.newCachedThreadPool();
		try {
			idxSystem.setBatchSize(10);

			final int count = DataForTests.testdata.length;
			List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(
					count);
			for (int i = 0; i < count; i++) {
				// list.add(new DataEvent<String>(i, TestData.testdata[i]));
				list.add(new DataEvent<String>(
						DataForTests.testdata[i], ""+i));
			}
			memoryProvider.addEvents(list);

			idxSystem.syncWithVersion(100000, "" + (count - 1));
			Future[] futures = new Future<?>[queryRunnables.length];
			for (int x = 0; x < queryRunnables.length; x++) {
				futures[x] = threadPool.submit(queryRunnables[x]);
			}

			for (int n = 1; n <= 3; n++) {
				for (int i = 0; i < count; i++) {
					// long version = n * count + i;
					// list = new ArrayList<DataEvent<String>>(1);
					// list.add(new DataEvent<String>(version,
					// TestData.testdata[i]));

					long version = n * count + i;

					list = new ArrayList<DataEvent<String>>(
							1);
					list.add(new DataEvent<String>(
							DataForTests.testdata[i], ""+version));

					memoryProvider.addEvents(list);

					idxSystem.syncWithVersion(100000, ""+version);
				}
				boolean stopNow = false;
				for (QueryRunnable queryThread : queryRunnables)
					stopNow |= queryThread.stop;
				if (stopNow)
					break;
			}
			for (QueryRunnable queryThread : queryRunnables)
				queryThread.stop = true; // stop all query threads
			for (int x = 0; x < queryRunnables.length; x++) {
				futures[x].get();
				assertTrue("count mismatch[" + queryRunnables[x].message + "]",
						!queryRunnables[x].mismatch);
			}
		} catch (Exception e) {
			for (QueryRunnable queryThread : queryRunnables) {
				if (queryThread.exception == null)
					throw new ZoieException(e);
			}
		} finally {
			memoryProvider.stop();
			for (String bname : idxSystem.getStandardMBeanNames()) {
				unregisterMBean(bname);
			}
			idxSystem.shutdown();
			deleteDirectory(idxDir);
		}
		System.out.println(" done round");
		log.info(" done round");
		for (QueryRunnable queryThread : queryRunnables) {
			if (queryThread.exception != null)
				throw new ZoieException(queryThread.exception);
		}
	}

	@Test
	public void testDelBigSet() throws ZoieException {
		for (int i = 0; i < 2; i++) {
			System.out.println("testDelBigSet Round: " + i);
			log.info("\n\n\ntestDelBigSet Round: " + i);
			testDelBigSetImpl();
		}
	}

	private void testDelBigSetImpl() throws ZoieException {
		long starttime = System.currentTimeMillis();
		final long testduration = 3000L; // one minute
		final long endtime = starttime + testduration;
		final int membatchsize = 1;
		File idxDir = getIdxDir();
		final int datacount = 100;
		final String[] testdata = new String[datacount];
		Random r = new Random(0);
		for (int i = 0; i < datacount; i++) {
			testdata[i] = "zoie " + (i % 2 == 0 ? "even " : "odd ") + i;
		}
		final ZoieSystem<IndexReader, String> idxSystem = createZoie(
				idxDir, true, 2, ZoieConfig.DEFAULT_VERSION_COMPARATOR);
		for (String bname : idxSystem.getStandardMBeanNames()) {
			registerMBean(idxSystem.getStandardMBean(bname), bname);
		}
		idxSystem.getAdminMBean().setFreshness(20);
		idxSystem.start();
		final String query = "zoie";
		int numThreads = 5;
		QueryThread[] queryThreads = new QueryThread[numThreads];
		for (int i = 0; i < queryThreads.length; i++) {
			queryThreads[i] = new QueryThread() {
				@Override
        public void run() {
					QueryParser parser = new QueryParser(
							Version.LUCENE_34, "contents",
							idxSystem.getAnalyzer());
					Query q;
					try {
						q = parser.parse(query);
					} catch (Exception e) {
						exception = e;
						return;
					}

					int expected = testdata.length;
					while (!stop) {
						Searcher searcher = null;
						List<ZoieIndexReader<IndexReader>> readers = null;
						MultiReader reader = null;
						try {
							readers = idxSystem.getIndexReaders();
							reader = new MultiReader(
									readers.toArray(new IndexReader[readers
											.size()]), false);

							searcher = new IndexSearcher(reader);

							TopDocs hits = searcher.search(q, 10);
							int count = hits.totalHits;

							if (count != expected) {
								mismatch = true;
								message = "hit count: " + count
										+ " / expected: " + expected;
								stop = true;
								StringBuffer sb = new StringBuffer();
								sb.append(message + "\n");
								sb.append("each\n");
								sb.append(groupDump(readers, q));
								sb.append("main\n");
								sb.append(dump(reader, hits));
								System.out.println(sb.toString());
								log.info(sb.toString());
							}
							Thread.sleep(2);
						} catch (Exception ex) {
							ex.printStackTrace();
							exception = ex;
							stop = true;
						} finally {
							try {
								if (searcher != null) {
									searcher.close();
									reader.close();
									reader = null;
									searcher = null;
								}
							} catch (IOException ioe) {
								log.error(ioe.getMessage(), ioe);
							} finally {
								idxSystem.returnIndexReaders(readers);
							}
						}
					}
				}

				private String groupDump(
						List<ZoieIndexReader<IndexReader>> readers, Query q)
						throws IOException {
					StringBuffer sb = new StringBuffer();
					for (ZoieIndexReader<IndexReader> reader : readers) {
						sb.append(reader).append("\n");
						Searcher searcher = new IndexSearcher(reader);
						TopDocs hits = searcher.search(q, 20);
						sb.append(dump(reader, hits));
						searcher.close();
						searcher = null;
					}
					return sb.toString();
				}

				private String dump(IndexReader reader, TopDocs hits)
						throws CorruptIndexException, IOException {
					StringBuffer sb = new StringBuffer();
					ScoreDoc[] sd = hits.scoreDocs;
					long[] uids = new long[sd.length];
					try {
						if (reader.hasDeletions())
							sb.append(" there are deletions @ version: "
									+ reader.getVersion());
					} catch (UnsupportedOperationException e) {
						if (reader.hasDeletions())
							sb.append(" there are deletions @ version: N/A");
					}
					sb.append("\n");
					for (int i = 0; i < sd.length; i++) {
						Document doc = reader.document(sd[i].doc);
						uids[i] = Long.parseLong(doc.get("id"));
						if (reader.isDeleted(sd[i].doc)) {
							sb.append(
									"doc: " + sd[i].doc + " with uid: "
											+ uids[i] + " has been deleted")
									.append("\n");
						}
					}
					sb.append(Thread.currentThread() + Arrays.toString(uids))
							.append("\n");
					int max = reader.maxDoc();
					uids = new long[max];
					for (int i = 0; i < max; i++) {
						Document doc = reader.document(i);
						uids[i] = Long.parseLong(doc.get("id"));
						if (reader.isDeleted(i)) {
							sb.append(
									"doc: " + i + " with uid: " + uids[i]
											+ " has been deleted").append("\n");
						}
					}
					sb.append("uids: " + Arrays.toString(uids)).append("\n");
					return sb.toString();
				}
			};
			queryThreads[i].setDaemon(true);
		}

		MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
		memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
		memoryProvider.setBatchSize(membatchsize);
		memoryProvider.setDataConsumer(idxSystem);
		memoryProvider.start();
		try {
			idxSystem.setBatchSize(10);

			final int count = testdata.length;
			List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(
					count);
			for (int i = 0; i < count; i++) {
				list.add(new DataEvent<String>(testdata[i],
						""+i));
			}
			memoryProvider.addEvents(list);

			idxSystem.syncWithVersion(1000000, ""+(count-1));

			for (QueryThread queryThread : queryThreads)
				queryThread.start();

			for (int n = 1; n <= 3; n++) {
				for (int i = 0; i < count; i++) {
					long version = n * count + i;
					list = new ArrayList<DataEvent<String>>(
							1);
					list.add(new DataEvent<String>(
							testdata[r.nextInt(testdata.length)], ""+version));
					memoryProvider.addEvents(list);

					idxSystem.syncWithVersion(100000, ""+version);
					if (System.currentTimeMillis() > endtime)
						break;
				}
				boolean stopNow = false;
				for (QueryThread queryThread : queryThreads)
					stopNow |= queryThread.stop;
				if (stopNow)
					break;
			}
			for (QueryThread queryThread : queryThreads)
				queryThread.stop = true; // stop all query threads
			for (QueryThread queryThread : queryThreads) {
				queryThread.join();
				assertTrue("count mismatch[" + queryThread.message + "]",
						!queryThread.mismatch);
			}
		} catch (Exception e) {
			for (QueryThread queryThread : queryThreads) {
				if (queryThread.exception == null)
					throw new ZoieException(e);
			}
		} finally {
			memoryProvider.stop();
			for (String bname : idxSystem.getStandardMBeanNames()) {
				unregisterMBean(bname);
			}
			idxSystem.shutdown();
			deleteDirectory(idxDir);
		}
		System.out.println(" done round");
		log.info(" done round");
		for (QueryThread queryThread : queryThreads) {
			if (queryThread.exception != null)
				throw new ZoieException(queryThread.exception);
		}
	}
}
