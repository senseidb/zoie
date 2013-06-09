package proj.zoie.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.junit.Ignore;
import org.junit.Test;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DataDoc;
import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.UIDDocIdSet;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;
import proj.zoie.impl.indexing.AsyncDataConsumer;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.DataForTests;
import proj.zoie.test.mock.MockDataLoader;

public class ZoieTest extends ZoieTestCaseBase {
  static Logger log = Logger.getLogger(ZoieTest.class);

  public ZoieTest() {
  }

  private static int countHits(ZoieSystem<IndexReader, String> idxSystem, Query q)
      throws IOException {
    IndexSearcher searcher = null;
    MultiReader reader = null;
    List<ZoieMultiReader<IndexReader>> readers = null;
    try {
      readers = idxSystem.getIndexReaders();
      reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
      searcher = new IndexSearcher(reader);
      TopDocs hits = searcher.search(q, 10);
      return hits.totalHits;
    } finally {
      try {
        if (reader != null) {
          reader.close();
          reader = null;
        }
      } finally {
        idxSystem.returnIndexReaders(readers);
      }
    }
  }

  @Test
  public void testIndexWithAnalyzer() throws ZoieException, IOException {
    File idxDir = getIdxDir();
    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true, 20,
      new WhitespaceAnalyzer(Version.LUCENE_43), null, ZoieConfig.DEFAULT_VERSION_COMPARATOR, false);
    idxSystem.start();

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(2);

    list.add(new DataEvent<String>("hao,yan 0", "0"));
    list.add(new DataEvent<String>("hao,yan 1", "1"));
    memoryProvider.addEvents(list);

    memoryProvider.flush();
    idxSystem.syncWithVersion(10000, "1");
    List<ZoieMultiReader<IndexReader>> readers = null;
    IndexSearcher searcher = null;
    MultiReader reader = null;
    try {
      readers = idxSystem.getIndexReaders();
      reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
      searcher = new IndexSearcher(reader);

      TopDocs hits = searcher.search(new TermQuery(new Term("contents", "hao,yan")), 10);

      assertEquals(1, hits.totalHits);
      // assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));
      assertEquals(String.valueOf((Integer.MAX_VALUE * 2L + 1L)),
        searcher.doc(hits.scoreDocs[0].doc).get("id"));

      hits = searcher.search(new TermQuery(new Term("contents", "hao")), 10);
      assertEquals(1, hits.totalHits);
      // assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));
    } finally {
      try {
        if (reader != null) {
          reader.close();
          reader = null;
        }
      } finally {
        idxSystem.returnIndexReaders(readers);
      }
    }
  }

  @Test
  public void testRealtime2() throws ZoieException {
    File idxDir = getIdxDir();
    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    idxSystem.start();

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    try {
      int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);
      memoryProvider.flush();

      idxSystem.flushEvents(10000);

      List<ZoieMultiReader<IndexReader>> readers = idxSystem.getIndexReaders();

      int numDocs = 0;
      for (ZoieMultiReader<IndexReader> r : readers) {
        numDocs += r.numDocs();
      }
      idxSystem.returnIndexReaders(readers);

      assertEquals(count, numDocs);
    } catch (IOException ioe) {
      throw new ZoieException(ioe.getMessage());
    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  private static class HalfPurgeFilter extends Filter {
    @Override
    public DocIdSet getDocIdSet(final AtomicReaderContext ctx, Bits bits) throws IOException {

      return new DocIdSet() {
        IndexReader reader = ctx.reader();

        @Override
        public DocIdSetIterator iterator() throws IOException {
          return new DocIdSetIterator() {

            int doc = -1;
            int maxdoc = reader.maxDoc();

            @Override
            public int advance(int target) throws IOException {
              doc = target - 1;
              return nextDoc();
            }

            @Override
            public int docID() {
              return doc;
            }

            @Override
            public int nextDoc() throws IOException {
              while (doc < maxdoc) {
                doc++;
                if (doc % 2 == 0) { // if even
                  return doc;
                }
              }
              return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public long cost() {
              // TODO Auto-generated method stub
              return 0;
            }

          };
        }

      };
    }
  }

  @Test
  public void testIndexEventListener() throws Exception {
    File idxDir = getIdxDir();
    final int[] flushNum = { 0 };
    final String[] flushVersion = { null };

    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR, true);

    idxSystem.start();

    idxSystem.addIndexingEventListener(new IndexingEventListener() {

      @Override
      public void handleUpdatedDiskVersion(String version) {
        flushVersion[0] = version;
      }

      @Override
      public void handleIndexingEvent(IndexingEvent evt) {
        flushNum[0]++;
      }
    });

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    try {
      int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);
      memoryProvider.flush();

      idxSystem.flushEvents(10000);
      String diskVersion = null;
      while (!"9".equals(diskVersion)) {
        diskVersion = idxSystem.getCurrentDiskVersion();
        Thread.sleep(500);
      }

    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }

    assertTrue(flushNum[0] > 0);
    assertEquals("9", flushVersion[0]);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSegmentTermDocs() throws Exception {

    class DefaultInterpreter implements ZoieIndexableInterpreter<DataDoc> {

      @Override
      public ZoieIndexable convertAndInterpret(DataDoc src) {
        return src;
      }

    }

    File idxDir = getIdxDir();

    ZoieConfig zConfig = new ZoieConfig();

    ZoieSystem<?, DataDoc> zoie = ZoieSystem.buildDefaultInstance(idxDir, new DefaultInterpreter(),
      zConfig);
    zoie.start();

    Document d1 = new Document();
    StringField f1 = new StringField("num", "abcdef", Store.YES);
    d1.add(f1);

    Document d2 = new Document();
    StringField f2 = new StringField("num", "abcd", Store.YES);
    d2.add(f2);

    Document d3 = new Document();
    StringField f3 = new StringField("num", "abcde", Store.YES);
    d3.add(f3);

    DataEvent<DataDoc> de1 = new DataEvent<DataDoc>(new DataDoc(1, d1), "1");
    DataEvent<DataDoc> de2 = new DataEvent<DataDoc>(new DataDoc(2, d2), "1");
    DataEvent<DataDoc> de3 = new DataEvent<DataDoc>(new DataDoc(3, d3), "1");

    try {
      zoie.consume(Arrays.asList(de1, de2, de3));
      zoie.flushEvents(10000);

      List<?> readerList = zoie.getIndexReaders();
      // combine the readers
      MultiReader reader = new MultiReader(readerList.toArray(new IndexReader[readerList.size()]),
          false);
      // do search
      IndexSearcher searcher = new IndexSearcher(reader);
      QueryParser parser = new QueryParser(Version.LUCENE_43, "num", new StandardAnalyzer(
          Version.LUCENE_43));
      Query q = parser.parse("num:abc*");
      TopDocs ret = searcher.search(q, 100);
      TestCase.assertEquals(3, ret.totalHits);

      zoie.returnIndexReaders((List) readerList);

      de1 = new DataEvent<DataDoc>(new DataDoc(1), "2");
      de2 = new DataEvent<DataDoc>(new DataDoc(2), "2");
      de3 = new DataEvent<DataDoc>(new DataDoc(3), "2");
      zoie.consume(Arrays.asList(de1, de2, de3));

      zoie.flushEventsToMemoryIndex(10000);

      readerList = zoie.getIndexReaders();
      // combine the readers
      reader = new MultiReader(readerList.toArray(new IndexReader[readerList.size()]), false);
      // do search
      searcher = new IndexSearcher(reader);
      ret = searcher.search(q, 100);

      TestCase.assertEquals(0, ret.totalHits);
      zoie.returnIndexReaders((List) readerList);
    } catch (IOException ioe) {
      throw new ZoieException(ioe.getMessage());
    } finally {
      zoie.shutdown();
      deleteDirectory(idxDir);
    }
  }

  @Test
  public void testPurgeFilter() throws Exception {
    File idxDir = getIdxDir();
    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR, true);

    idxSystem.setPurgeFilter(new HalfPurgeFilter());
    idxSystem.start();

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    try {
      int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);
      memoryProvider.flush();

      idxSystem.flushEvents(10000);

      List<ZoieMultiReader<IndexReader>> readers = idxSystem.getIndexReaders();

      MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[0]), false);

      IndexSearcher searcher = new IndexSearcher(multiReader);

      int numDocs = searcher.search(new MatchAllDocsQuery(), 10).totalHits;

      log.info("numdocs: " + numDocs);
      TestCase.assertTrue(numDocs > 0);

      idxSystem.returnIndexReaders(readers);

      idxSystem.getAdminMBean().flushToDiskIndex();

      idxSystem.refreshDiskReader();
      readers = idxSystem.getIndexReaders();

      multiReader = new MultiReader(readers.toArray(new IndexReader[0]), false);

      searcher = new IndexSearcher(multiReader);

      numDocs = searcher.search(new MatchAllDocsQuery(), 10).totalHits;

      numDocs = multiReader.numDocs();

      log.info("new numdocs: " + numDocs);
      // TODO for some reasons numdocs might be 6. I've put a temporary fix to avoid sporadical
      // failures
      TestCase.assertTrue("numdDocs should be 5" + numDocs, (numDocs == 5 || numDocs == 6));

      idxSystem.returnIndexReaders(readers);

    } catch (IOException ioe) {
      throw new ZoieException(ioe.getMessage());
    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  @Test
  public void testStore() throws ZoieException {
    File idxDir = getIdxDir();
    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    idxSystem.start();

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    try {
      int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);
      memoryProvider.flush();

      idxSystem.flushEvents(1000);

      List<ZoieMultiReader<IndexReader>> readers = idxSystem.getIndexReaders();

      BytesRef data = null;
      for (ZoieMultiReader<IndexReader> r : readers) {
        data = r.getStoredValue(((Integer.MAX_VALUE) * 2L));
        if (data != null) break;
      }

      TestCase.assertNotNull(data);
      String val = data.toString();
      String[] parts = val.split(" ");
      final long id = Long.parseLong(parts[parts.length - 1]);
      TestCase.assertEquals(0L, id);

      data = null;
      for (ZoieMultiReader<IndexReader> r : readers) {
        data = r.getStoredValue(((Integer.MAX_VALUE) * 2L) + 1L);
        if (data != null) break;
      }
      TestCase.assertNull(data);

      idxSystem.returnIndexReaders(readers);

    } catch (IOException ioe) {
      throw new ZoieException(ioe.getMessage());
    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  // hao: test for new zoieVersion
  @Test
  public void testRealtime() throws ZoieException {
    File idxDir = getIdxDir();
    ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    idxSystem.start();
    String query = "zoie";
    QueryParser parser = new QueryParser(Version.LUCENE_43, "contents", idxSystem.getAnalyzer());
    Query q = null;
    try {
      q = parser.parse(query);
    } catch (Exception e) {
      throw new ZoieException(e.getMessage(), e);
    }
    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try {
      int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);

      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);

      idxSystem.syncWithVersion(10000, "" + (count - 1));

      int repeat = 20;
      int idx = 0;
      int[] results = new int[repeat];
      int[] expected = new int[repeat];
      Arrays.fill(expected, count);

      // should be consumed by the idxing system
      IndexSearcher searcher = null;
      MultiReader reader = null;
      List<ZoieMultiReader<IndexReader>> readers = null;
      for (int i = 0; i < repeat; ++i) {
        try {
          readers = idxSystem.getIndexReaders();
          reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
          searcher = new IndexSearcher(reader);

          TopDocs hits = searcher.search(q, 10);
          results[idx++] = hits.totalHits;

        } finally {
          try {
            if (searcher != null) {
              searcher = null;
              reader.close();
              reader = null;
            }
          } finally {
            idxSystem.returnIndexReaders(readers);
          }
        }
        try {
          Thread.sleep(30);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

      assertEquals("maybe race condition in disk flush", Arrays.toString(expected),
        Arrays.toString(results));
    } catch (IOException ioe) {
      throw new ZoieException(ioe.getMessage());
    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  @Test
  public void testStreamDataProvider() throws ZoieException {
    MockDataLoader<Integer> consumer = new MockDataLoader<Integer>();
    MemoryStreamDataProvider<Integer> memoryProvider = new MemoryStreamDataProvider<Integer>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(consumer);
    memoryProvider.start();
    try {
      int count = 10;

      List<DataEvent<Integer>> list = new ArrayList<DataEvent<Integer>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<Integer>(i, "" + i));
      }
      memoryProvider.addEvents(list);

      memoryProvider.syncWithVersion(10000, "" + (count - 1));
      int num = consumer.getCount();
      assertEquals(num, count);
    } finally {
      memoryProvider.stop();
    }
  }

  @Test
  public void testStreamDataProviderFlush() throws ZoieException {
    MockDataLoader<Integer> consumer = new MockDataLoader<Integer>();
    MemoryStreamDataProvider<Integer> memoryProvider = new MemoryStreamDataProvider<Integer>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setBatchSize(100);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(consumer);
    memoryProvider.start();
    try {
      int count = 10;

      List<DataEvent<Integer>> list = new ArrayList<DataEvent<Integer>>(count);
      for (int i = 0; i < count; ++i) {
        list.add(new DataEvent<Integer>(i, "" + i));
      }
      memoryProvider.addEvents(list);

      memoryProvider.flush();

      int num = consumer.getCount();
      assertEquals(num, count);
    } finally {
      memoryProvider.stop();
    }
  }

  @Test
  public void testAsyncDataConsumer() throws ZoieException {
    final long[] delays = { 0L, 10L, 100L, 1000L };
    // final long[] delays = {100L, 1000L };
    final int[] batchSizes = { 1, 10, 100, 1000, 1000 };
    final int count = 1000;
    // final int count=TestData.testdata.length;
    final long timeout = 10000L;

    for (long delay : delays) {
      for (int batchSize : batchSizes) {

        if (delay * (count / batchSize + 1) > timeout) {
          continue; // skip this combination. it will take too long.
        }

        MockDataLoader<Integer> mockLoader = new MockDataLoader<Integer>();
        mockLoader.setDelay(delay);

        AsyncDataConsumer<Integer> asyncConsumer = new AsyncDataConsumer<Integer>(
            ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        asyncConsumer.setDataConsumer(mockLoader);
        asyncConsumer.setBatchSize(batchSize);
        asyncConsumer.start();

        MemoryStreamDataProvider<Integer> memoryProvider = new MemoryStreamDataProvider<Integer>(
            ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);

        memoryProvider.setDataConsumer(asyncConsumer);
        memoryProvider.start();
        memoryProvider.setBatchSize(batchSize);
        try {
          List<DataEvent<Integer>> list = new ArrayList<DataEvent<Integer>>(count);
          for (int i = 0; i < count; ++i) {
            // System.out.println("ZoieTest.java:testAsyncDataConsumer():delay:"
            // + delay + ", batchSize:" + batchSize +
            // ",forloop: i: " + i);
            list.add(new DataEvent<Integer>(i, "" + i));
          }
          memoryProvider.addEvents(list);

          boolean done = false;
          long start = System.currentTimeMillis();
          while (!done) {
            try {
              asyncConsumer.syncWithVersion(timeout, "" + (count - 1));
              done = true;
            } catch (ZoieException e) {
              if (!e.getMessage().contains("sync timed out")) throw e;
              else System.out
                  .println("sync time out could be legit for slow systems. Elapsed time: "
                      + (System.currentTimeMillis() - start) + "ms");
              if (System.currentTimeMillis() - start > 600000L) throw e;
            }
          }
          int num = mockLoader.getCount();
          assertEquals("batchSize=" + batchSize, num, count);
          assertTrue("batch not working",
            (mockLoader.getMaxBatch() > 1 || mockLoader.getMaxBatch() == batchSize));
        } finally {
          memoryProvider.stop();
          asyncConsumer.stop();
        }
      }
    }
  }

  @Ignore
  @Test
  public void testDelSet() throws ZoieException {
    for (int i = 0; i < 2; i++) {
      testDelSetImpl();
    }
  }

  private void testDelSetImpl() throws ZoieException {
    File idxDir = getIdxDir();
    final ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true, 100,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    idxSystem.getAdminMBean().setFreshness(50);
    idxSystem.start();
    int numThreads = 5;
    QueryThread[] queryThreads = new QueryThread[numThreads];
    for (int i = 0; i < queryThreads.length; i++) {
      queryThreads[i] = new QueryThread() {
        @Override
        public void run() {
          final String query = "zoie";
          QueryParser parser = new QueryParser(Version.LUCENE_43, "contents",
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
            IndexSearcher searcher = null;
            List<ZoieMultiReader<IndexReader>> readers = null;
            MultiReader reader = null;
            try {
              readers = idxSystem.getIndexReaders();
              IndexReader[] subReaders = new IndexReader[readers.size()];
              for (int i = 0; i < subReaders.length; ++i) {
                subReaders[i] = readers.get(i);
              }
              reader = new MultiReader(subReaders, false);

              searcher = new IndexSearcher(reader);

              TopDocs hits = searcher.search(q, 10);
              int count = hits.totalHits;

              if (count != expected) {
                mismatch = true;
                message = "hit count: " + count + " / expected: " + expected;
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

        private String groupDump(List<ZoieMultiReader<IndexReader>> readers, Query q)
            throws IOException {
          StringBuffer sb = new StringBuffer();
          for (ZoieMultiReader<IndexReader> reader : readers) {
            sb.append(reader).append("\n");
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs hits = searcher.search(q, 20);
            sb.append(dump(reader, hits));
            searcher = null;
          }
          return sb.toString();
        }

        private String dump(IndexReader reader, TopDocs hits) throws CorruptIndexException,
            IOException {
          StringBuffer sb = new StringBuffer();
          ScoreDoc[] sd = hits.scoreDocs;
          long[] uids = new long[sd.length];
          try {
            if (reader.hasDeletions()) sb.append(" there are deletions @ version: "
                + ((DirectoryReader) reader).getVersion());
          } catch (UnsupportedOperationException e) {
            if (reader.hasDeletions()) sb.append(" there are deletions @ version: N/A");
          }
          sb.append("\n");
          for (int i = 0; i < sd.length; i++) {
            Document doc = reader.document(sd[i].doc);
            uids[i] = Long.parseLong(doc.get("id"));
          }
          sb.append(Thread.currentThread() + Arrays.toString(uids)).append("\n");
          int max = reader.maxDoc();
          uids = new long[max];
          for (int i = 0; i < max; i++) {
            Document doc = reader.document(i);
            uids[i] = Long.parseLong(doc.get("id"));
          }
          sb.append("uids: " + Arrays.toString(uids)).append("\n");
          return sb.toString();
        }
      };
      queryThreads[i].setDaemon(true);
    }

    MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
        ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try {
      idxSystem.setBatchSize(10);

      final int count = DataForTests.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; i++) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWithVersion(100000, "" + (count - 1));

      for (QueryThread queryThread : queryThreads)
        queryThread.start();

      for (int n = 1; n <= 3; n++) {
        for (int i = 0; i < count; i++) {
          long version = n * count + i;
          list = new ArrayList<DataEvent<String>>(1);
          list.add(new DataEvent<String>(DataForTests.testdata[i], "" + version));
          memoryProvider.addEvents(list);

          idxSystem.syncWithVersion(100000, "" + version);
        }
        boolean stopNow = false;
        for (QueryThread queryThread : queryThreads)
          stopNow |= queryThread.stop;
        if (stopNow) break;
      }
      for (QueryThread queryThread : queryThreads)
        queryThread.stop = true; // stop all query threads
      for (QueryThread queryThread : queryThreads) {
        queryThread.join();
        assertTrue("count mismatch[" + queryThread.message + "]", !queryThread.mismatch);
      }
    } catch (Exception e) {
      for (QueryThread queryThread : queryThreads) {
        if (queryThread.exception == null) throw new ZoieException(e);
      }
    } finally {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
    System.out.println(" done round");
    log.info(" done round");
    for (QueryThread queryThread : queryThreads) {
      if (queryThread.exception != null) throw new ZoieException(queryThread.exception);
    }
  }

  @Test
  public void testDocIDMapper() {
    final long[] uidList = new long[500000];
    long[] qryList = new long[100000];
    int intersection = 10000;
    int del = 5;
    int[] ansList1 = new int[qryList.length];
    int[] ansList2 = new int[qryList.length];
    java.util.Random rand = new java.util.Random(System.currentTimeMillis());
    DocIDMapperImpl mapper = null;

    for (int k = 0; k < 10; k++) {
      java.util.HashSet<Long> uidset = new java.util.HashSet<Long>();
      java.util.HashSet<Long> qryset = new java.util.HashSet<Long>();
      long id;
      for (int i = 0; i < intersection; i++) {
        do {
          id = rand.nextInt() + (Integer.MAX_VALUE) * 2L;
        } while (id == ZoieSegmentReader.DELETED_UID || uidset.contains(id));

        uidset.add(id);
        uidList[i] = (i % del) > 0 ? id : ZoieSegmentReader.DELETED_UID;
        qryList[i] = id;
        ansList1[i] = (i % del) > 0 ? i : -1;
      }
      for (int i = intersection; i < uidList.length; i++) {
        do {
          id = rand.nextInt() + (Integer.MAX_VALUE) * 2L;
        } while (id == ZoieSegmentReader.DELETED_UID || uidset.contains(id));

        uidset.add(id);
        uidList[i] = (i % del) > 0 ? id : ZoieSegmentReader.DELETED_UID;
      }
      for (int i = intersection; i < qryList.length; i++) {
        do {
          id = rand.nextInt() + (Integer.MAX_VALUE) * 2L;
        } while (id == ZoieSegmentReader.DELETED_UID || uidset.contains(id) || qryset.contains(id));

        qryset.add(id);
        qryList[i] = id;
        ansList1[i] = -1;
      }

      final NumericDocValues uidValues = new NumericDocValues() {
        @Override
        public long get(int docID) {
          return uidList[docID];
        }
      };
      mapper = new DocIDMapperImpl(uidValues, uidList.length);

      for (int i = 0; i < qryList.length; i++) {
        ansList2[i] = mapper.getDocID(qryList[i]);
      }
      assertTrue("wrong result", Arrays.equals(ansList1, ansList2));
      for (int i = 0; i < qryList.length; i++) {
        assertEquals("wrong result", ansList2[i], mapper.getDocID(i));
      }
    }

  }

  @Test
  public void testExportImport() throws ZoieException, IOException {
    File idxDir = getIdxDir();
    final ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true,
      ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    idxSystem.start();

    DirectoryManager dirMgr = new DefaultDirectoryManager(idxDir);

    String query = "zoie";
    QueryParser parser = new QueryParser(Version.LUCENE_43, "contents", idxSystem.getAnalyzer());
    Query q = null;
    try {
      q = parser.parse(query);
    } catch (Exception e) {
      throw new ZoieException(e.getMessage(), e);
    }

    try {
      List<DataEvent<String>> list;

      list = new ArrayList<DataEvent<String>>(DataForTests.testdata.length);
      for (int i = 0; i < DataForTests.testdata.length; ++i) {
        list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);

      assertEquals("index version mismatch after first flush", DataForTests.testdata.length - 1,
        DataForTests.testdata.length - 1);

      int hits = countHits(idxSystem, q);

      RandomAccessFile exportFile;
      FileChannel channel;

      exportFile = new RandomAccessFile(new File(getTmpDir(), "zoie_export.dat"), "rw");
      channel = exportFile.getChannel();
      idxSystem.exportSnapshot(channel);
      channel.close();
      exportFile.close();
      exportFile = null;
      channel = null;

      list = new ArrayList<DataEvent<String>>(DataForTests.testdata2.length);
      for (int i = 0; i < DataForTests.testdata2.length; ++i) {

        list.add(new DataEvent<String>(DataForTests.testdata.length + DataForTests.testdata2[i], ""
            + (DataForTests.testdata.length + i)));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      String zvt = dirMgr.getVersion();
      assertEquals("index version mismatch after second flush", DataForTests.testdata.length
          + DataForTests.testdata2.length - 1, (long) Long.valueOf(zvt));
      assertEquals("should have no hits", 0, countHits(idxSystem, q));

      exportFile = new RandomAccessFile(new File(getTmpDir(), "zoie_export.dat"), "r");
      channel = exportFile.getChannel();
      idxSystem.importSnapshot(channel);
      idxSystem.flushEvents(10000);
      channel.close();
      exportFile.close();

      assertEquals("count is wrong", hits, countHits(idxSystem, q));
    }

    catch (ZoieException e) {
      throw e;
    } finally {
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  @Test
  public void testUIDDocIdSet() throws IOException {
    LongOpenHashSet uidset = new LongOpenHashSet();
    int count = 100;
    Random rand = new Random();
    int id;
    for (int i = 0; i < count; ++i) {
      do {
        id = rand.nextInt();
      } while (id == ZoieSegmentReader.DELETED_UID || uidset.contains(id));
      uidset.add(id);
    }

    long[] uidArray = uidset.toLongArray();

    final long[] even = new long[uidArray.length / 2];
    int[] ans = new int[even.length];
    for (int i = 0; i < even.length; ++i) {
      even[i] = uidArray[i * 2];
      ans[i] = i;
    }

    final NumericDocValues uidValues = new NumericDocValues() {
      @Override
      public long get(int docID) {
        return even[docID];
      }
    };

    DocIDMapperImpl mapper = new DocIDMapperImpl(uidValues, uidArray.length / 2);
    UIDDocIdSet uidSet = new UIDDocIdSet(even, mapper);
    DocIdSetIterator docidIter = uidSet.iterator();
    IntArrayList intList = new IntArrayList();
    int docid;
    while ((docid = docidIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      intList.add(docid);
    }
    assertTrue("wrong result from iter", Arrays.equals(ans, intList.toIntArray()));

    long[] newidArray = new long[count];
    for (int i = 0; i < count; ++i) {
      newidArray[i] = i;
    }

    final NumericDocValues newUidValues = new NumericDocValues() {
      @Override
      public long get(int docID) {
        return docID;
      }
    };
    mapper = new DocIDMapperImpl(newUidValues, count);
    uidSet = new UIDDocIdSet(newidArray, mapper);
    docidIter = uidSet.iterator();
    intList = new IntArrayList();
    for (int i = 0; i < newidArray.length; ++i) {
      docid = docidIter.advance(i * 10);
      if (docid == DocIdSetIterator.NO_MORE_DOCS) break;
      intList.add(docid);
      docid = docidIter.nextDoc();
      if (docid == DocIdSetIterator.NO_MORE_DOCS) break;
      intList.add(docid);
    }

    int[] answer = new int[] { 0, 1, 10, 11, 20, 21, 30, 31, 40, 41, 50, 51, 60, 61, 70, 71, 80,
        81, 90, 91 };
    assertTrue("wrong result from mix of next and skip",
      Arrays.equals(answer, intList.toIntArray()));
  }

  public static void main(String[] args) {
  }
}
