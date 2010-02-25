package proj.zoie.test;

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

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.UIDDocIdSet;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.AsyncDataConsumer;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.impl.indexing.internal.IndexReaderDispenser;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.test.data.TestData;
import proj.zoie.test.data.TestDataInterpreter;
import proj.zoie.test.mock.MockDataLoader;

public class ZoieTest extends ZoieTestCase
{
  static Logger logger = Logger.getLogger(ZoieTest.class);
  
  public ZoieTest() {
  }

  public ZoieTest(String name) {
    super(name);
  }

  @Override
  public void setUp()
  {
    System.out.println("executing test case: " + getName());
  }
  @Override
  public void tearDown()
  {
    deleteDirectory(getIdxDir());
  }
  private static File getIdxDir()
  {
    File tmpDir=new File(System.getProperty("java.io.tmpdir"));
    File tempFile = new File(tmpDir, "test-idx");
    int i = 0;
    while (tempFile.exists())
    {
      if (i>10)
      {
        System.out.println("cannot delete");
        return tempFile;
      }
      System.out.println("deleting " + tempFile);
      tempFile.delete();
      try
      {
        Thread.sleep(50);
      } catch(Exception e)
      {
        logger.error("thread interrupted in sleep in deleting file" + e);
      }
      i++;
    }
    return tempFile;
  }

  private static File getTmpDir()
  {
    return new File(System.getProperty("java.io.tmpdir"));
  }

  private static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime)
  {
    return createZoie(idxDir, realtime, 20);
  }

  private static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime, long delay)
  {
    return createZoie(idxDir,realtime,delay,null);
  }

  private static class TestIndexReaderDecorator implements IndexReaderDecorator<IndexReader>{
    public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader) throws IOException {
      return indexReader;
    }

    public IndexReader redecorate(IndexReader decorated,ZoieIndexReader<IndexReader> copy) throws IOException {
      return decorated;
    }
  }

  private static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime, long delay,Analyzer analyzer)
  {
    ZoieSystem<IndexReader,String> idxSystem=new ZoieSystem<IndexReader, String>(idxDir,new TestDataInterpreter(delay,analyzer),
        new TestIndexReaderDecorator(),null,null,50,100,realtime);
    return idxSystem;
  }

  private static boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
        if(files[i].isDirectory()) {
          deleteDirectory(files[i]);
        }
        else {
          files[i].delete();
        }
      }
    }
    return( path.delete() );
  }

  /*private static Searcher getSearcher(ZoieSystem<ZoieIndexReader,String> zoie) throws IOException
	{
		List<ZoieIndexReader> readers=zoie.getIndexReaders();
		MultiReader reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

		IndexSearcher searcher=new IndexSearcher(reader);
		return searcher;
	}
   */
  private static int countHits(ZoieSystem<IndexReader,String> idxSystem, Query q) throws IOException
  {
    Searcher searcher = null;
    MultiReader reader= null;
    List<ZoieIndexReader<IndexReader>> readers = null;
    try
    {
      readers=idxSystem.getIndexReaders();
      reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);

      TopDocs hits = searcher.search(q,10);

      return hits.totalHits;
    }
    finally
    {
      try{
        if (searcher != null){
          searcher.close();
          searcher = null;
          reader.close();
          reader = null;
        }
      }
      finally{
        idxSystem.returnIndexReaders(readers);
      }
    }
  }

  public void testIndexWithAnalyzer() throws ZoieException,IOException{
    File idxDir=getIdxDir();
    ZoieSystem<IndexReader,String> idxSystem=createZoie(idxDir,true,20,new WhitespaceAnalyzer());
    idxSystem.start();

    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    List<DataEvent<String>> list=new ArrayList<DataEvent<String>>(2);
    list.add(new DataEvent<String>(0,"john,wang 0"));
    list.add(new DataEvent<String>(1,"john,wang 1"));
    memoryProvider.addEvents(list);

    memoryProvider.flush();

    idxSystem.syncWthVersion(10000, 1);
    List<ZoieIndexReader<IndexReader>> readers = null;
    Searcher searcher =null;
    MultiReader reader = null;
    try
    {
      readers=idxSystem.getIndexReaders();
      reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);

      TopDocs hits=searcher.search(new TermQuery(new Term("contents","john,wang")),10);

      assertEquals(1,hits.totalHits);
      assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L+1L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));

      hits=searcher.search(new TermQuery(new Term("contents","john")),10);

      assertEquals(1,hits.totalHits);
      assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));
    }
    finally
    {
      try{
        if (searcher != null){
          searcher.close();
          searcher = null;
          reader.close();
          reader = null;
        }
      }
      finally{
        idxSystem.returnIndexReaders(readers);
      }
    }	
  }

  public void testRealtime() throws ZoieException
  {
    File idxDir=getIdxDir();
    ZoieSystem<IndexReader,String> idxSystem=createZoie(idxDir,true);
    idxSystem.start();
    String query="zoie";
    QueryParser parser=new QueryParser(Version.LUCENE_CURRENT,"contents",idxSystem.getAnalyzer());
    Query q=null;
    try 
    {
      q=parser.parse(query);
    } catch (ParseException e) {
      throw new ZoieException(e.getMessage(),e);
    }
    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      int count=TestData.testdata.length;
      List<DataEvent<String>> list=new ArrayList<DataEvent<String>>(count);
      for (int i=0;i<count;++i)
      {
        list.add(new DataEvent<String>(i,TestData.testdata[i]));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, count-1);

      int repeat = 20;
      int idx = 0;
      int[] results = new int[repeat];
      int[] expected = new int[repeat];
      Arrays.fill(expected, count);

      // should be consumed by the idxing system
      Searcher searcher=null;
      MultiReader reader= null;
      List<ZoieIndexReader<IndexReader>> readers = null;
      for (int i=0;i<repeat;++i)
      {
        try
        {
          readers=idxSystem.getIndexReaders();
          reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

          searcher=new IndexSearcher(reader);

          TopDocs hits=searcher.search(q,10);
          results[idx++] = hits.totalHits;

        }
        finally
        {
          try{
            if (searcher != null){
              searcher.close();
              searcher = null;
              reader.close();
              reader = null;
            }
          }
          finally{
            idxSystem.returnIndexReaders(readers);
          }
        }	
        try {
          Thread.sleep(30);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }	
      }

      assertEquals("maybe race condition in disk flush", Arrays.toString(expected), Arrays.toString(results));
    }
    catch(IOException ioe)
    {
      throw new ZoieException(ioe.getMessage());
    }
    finally
    {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }	
  }

  public void testStreamDataProvider() throws ZoieException
  {
    MockDataLoader<Integer> consumer=new MockDataLoader<Integer>();
    MemoryStreamDataProvider<Integer> memoryProvider=new MemoryStreamDataProvider<Integer>();
    memoryProvider.setDataConsumer(consumer);
    memoryProvider.start();
    try
    {
      int count=10;

      List<DataEvent<Integer>> list=new ArrayList<DataEvent<Integer>>(count);
      for (int i=0;i<count;++i)
      {
        list.add(new DataEvent<Integer>(i,i));
      }
      memoryProvider.addEvents(list);

      memoryProvider.syncWthVersion(10000, count-1);
      int num=consumer.getCount();
      assertEquals(num, count);   
    }
    finally
    {
      memoryProvider.stop();
    }
  }

  public void testAsyncDataConsumer() throws ZoieException
  {
    final long[] delays = { 0L, 10L, 100L, 1000L };
    final int[] batchSizes = { 1, 10, 100, 1000, 1000 };
    final int count=1000;
    final long timeout = 10000L;

    for(long delay : delays)
    {
      for(int batchSize : batchSizes)
      {

        if(delay * (count / batchSize + 1) > timeout)
        {
          continue; // skip this combination. it will take too long.
        }

        MockDataLoader<Integer> mockLoader=new MockDataLoader<Integer>();
        mockLoader.setDelay(delay);

        AsyncDataConsumer<Integer> asyncConsumer = new AsyncDataConsumer<Integer>();
        asyncConsumer.setDataConsumer(mockLoader);
        asyncConsumer.setBatchSize(batchSize);
        asyncConsumer.start();

        MemoryStreamDataProvider<Integer> memoryProvider=new MemoryStreamDataProvider<Integer>();
        memoryProvider.setDataConsumer(asyncConsumer);
        memoryProvider.start();
        memoryProvider.setBatchSize(batchSize);
        try
        {
          List<DataEvent<Integer>> list=new ArrayList<DataEvent<Integer>>(count);
          for (int i=0;i<count;++i)
          {
            list.add(new DataEvent<Integer>(i,i));
          }
          memoryProvider.addEvents(list);

          asyncConsumer.syncWthVersion(timeout, (long)(count-1));
          int num=mockLoader.getCount();
          assertEquals("batchSize="+batchSize, num, count);
          assertTrue("batch not working", (mockLoader.getMaxBatch() > 1 || mockLoader.getMaxBatch() == batchSize));
        }
        finally
        {
          memoryProvider.stop();
          asyncConsumer.stop();
        }
      }
    }
  }

  private class QueryThread extends Thread
  {
    public volatile boolean stop = false;
    public volatile boolean mismatch = false;
    public volatile String message = null;
    public Exception exception = null;
  }

  public void testDelSet() throws ZoieException
  {
    for(int i=0; i<10; i++)
    {
      System.out.println("testDelSet Round: " + i);
      testDelSetImpl();
    }
  }

  private void testDelSetImpl() throws ZoieException
  {
    File idxDir = getIdxDir();
    final ZoieSystem<IndexReader,String> idxSystem = createZoie(idxDir,true, 100);
    idxSystem.start();
    final String query = "zoie";
    int numThreads = 5;
    QueryThread[] queryThreads = new QueryThread[numThreads];
    for(int i = 0; i < queryThreads.length; i++)
    {
      queryThreads[i] = new QueryThread()
      {
        public void run()
        {
          QueryParser parser = new QueryParser(Version.LUCENE_CURRENT,"contents",idxSystem.getAnalyzer());
          Query q;
          try
          {
            q = parser.parse(query);
          }
          catch (Exception e)
          {
            exception = e;
            return;
          }

          int expected = TestData.testdata.length;
          while(!stop)
          {
            Searcher searcher = null;
            List<ZoieIndexReader<IndexReader>> readers = null;
            MultiReader reader=null;
            try
            {
              readers=idxSystem.getIndexReaders();
              reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

              searcher=new IndexSearcher(reader);

              TopDocs hits = searcher.search(q,10);
              int count = hits.totalHits;

              if (count != expected)
              {
                mismatch = true;
                message = "hit count: " + count +" / expected: "+expected;
                stop = true;
                StringBuffer sb = new StringBuffer();
                sb.append(message + "\n");
                sb.append("each\n");
                sb.append(groupDump(readers, q));
                sb.append("main\n");
                sb.append(dump(reader, hits));
                System.out.println(sb.toString());
              }
            }
            catch(Exception ex)
            {
              ex.printStackTrace();
              exception = ex;
              stop = true;
            }
            finally
            {
              try{
                if (searcher != null){
                  searcher.close();
                  reader.close();
                  reader = null;
                  searcher = null;
                }
              }
              catch(IOException ioe){
                logger.error(ioe.getMessage(),ioe);
              }
              finally{
                idxSystem.returnIndexReaders(readers);
              }
            }
          }
        }
        private String groupDump(List<ZoieIndexReader<IndexReader>> readers, Query q) throws IOException
        {
          StringBuffer sb = new StringBuffer();
          for(ZoieIndexReader<IndexReader> reader : readers)
          {
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
        throws CorruptIndexException, IOException
        {
          StringBuffer sb = new StringBuffer();
          ScoreDoc[] sd = hits.scoreDocs;
          long[] uids = new long[sd.length];
          try
          {
            if (reader.hasDeletions()) sb.append(" there are deletions @ version: " + reader.getVersion());
          } catch(UnsupportedOperationException e)
          {
            if (reader.hasDeletions()) sb.append(" there are deletions @ version: N/A");
          }
            sb.append("\n");
          for(int i =0; i< sd.length; i++)
          {
            Document doc = reader.document(sd[i].doc);
            uids[i] = Long.parseLong(doc.get("id"));
            if (reader.isDeleted(sd[i].doc))
            {
              sb.append("doc: " + sd[i].doc + " with uid: " +
                  uids[i] + " has been deleted").append("\n");
            }
          }
          sb.append(Thread.currentThread() + Arrays.toString(uids)).append("\n");
          int max = reader.maxDoc();
          uids = new long[max];
          for(int i=0; i<max; i++)
          {
            Document doc = reader.document(i);
            uids[i] = Long.parseLong(doc.get("id"));
            if (reader.isDeleted(i))
            {
              sb.append("doc: " + i + " with uid: " + uids[i] + " has been deleted").append("\n");
            }
          }
          sb.append("uids: " + Arrays.toString(uids)).append("\n");
          return sb.toString();
        }
      };
      queryThreads[i].setDaemon(false);
    }

    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      idxSystem.setBatchSize(10);

      final int count = TestData.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; i++)
      {
        list.add(new DataEvent<String>(i, TestData.testdata[i]));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(100000, count - 1);

      for(QueryThread queryThread : queryThreads) queryThread.start();

      for(int n = 1; n <= 3; n++)
      {
        for (int i = 0; i < count; i++)
        {
          long version = n * count + i;
          list = new ArrayList<DataEvent<String>>(1);
          list.add(new DataEvent<String>(version, TestData.testdata[i]));
          memoryProvider.addEvents(list);
          idxSystem.syncWthVersion(100000, version);
        }
        boolean stopNow = false;
        for(QueryThread queryThread : queryThreads) stopNow |= queryThread.stop;
        if(stopNow) break;
      }
      for(QueryThread queryThread : queryThreads) queryThread.stop = true; // stop all query threads
      for(QueryThread queryThread : queryThreads)
      {
        queryThread.join();
        assertTrue("count mismatch["+queryThread.message +"]", !queryThread.mismatch);
      }
    }
    catch(Exception e)
    {
      for(QueryThread queryThread : queryThreads)
      {
        if(queryThread.exception == null) throw new ZoieException(e);
      }
    }
    finally
    {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
    System.out.println(" done round");
    for(QueryThread queryThread : queryThreads)
    {
      if(queryThread.exception != null) throw new ZoieException(queryThread.exception);
    }
  }

  public void testUpdates() throws ZoieException, ParseException, IOException
  {
    File idxDir = getIdxDir();
    final ZoieSystem<IndexReader,String> idxSystem = createZoie(idxDir,true);
    idxSystem.start();
    final String query = "zoie";

    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      idxSystem.setBatchSize(10);

      long version = 0;
      final int count = TestData.testdata.length;
      List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
      for (int i = 0; i < count; i++)
      {
        version = i;
        list.add(new DataEvent<String>(i, TestData.testdata[i]));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, version);

      QueryParser parser = new QueryParser(Version.LUCENE_CURRENT,"contents",idxSystem.getAnalyzer());
      Query q;
      Searcher searcher = null;
      List<ZoieIndexReader<IndexReader>> readers = null;

      TopDocs hits;

      q = parser.parse("zoie");
      readers=idxSystem.getIndexReaders();
      MultiReader reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);
      hits = searcher.search(q,10);
      int expected = TestData.testdata.length;
      assertEquals("before update: zoie count mismatch[hit count: " + hits.totalHits +" / expected: "+TestData.testdata.length +"]", count, hits.totalHits);
      q = parser.parse("zoie2");

      searcher.close();
      reader.close();
      idxSystem.returnIndexReaders(readers);

      readers=idxSystem.getIndexReaders();
      reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);
      hits = searcher.search(q,10);
      assertEquals("before update: zoie2 count mismatch[hit count: " + hits.totalHits +" / expected: "+ 0 +"]", 0, hits.totalHits);
      searcher.close();
      reader.close();
      idxSystem.returnIndexReaders(readers);

      list = new ArrayList<DataEvent<String>>(TestData.testdata2.length);
      for(int i = 0; i < TestData.testdata2.length; i++)
      {
        version = count + i;
        list.add(new DataEvent<String>(version, TestData.testdata2[i]));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, version);

      q = parser.parse("zoie");
      readers=idxSystem.getIndexReaders();
      reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);
      hits = searcher.search(q,10);
      expected = 0;
      assertEquals("after update: zoie count mismatch[hit count: " + hits.totalHits +" / expected: "+ 0 +"]", 0,  hits.totalHits);
      searcher.close();
      reader.close();
      idxSystem.returnIndexReaders(readers);

      q = parser.parse("zoie2");

      readers=idxSystem.getIndexReaders();
      reader=new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);

      searcher=new IndexSearcher(reader);

      hits = searcher.search(q,10);
      expected = TestData.testdata2.length;
      assertEquals("after update: zoie2 count mismatch[hit count: " + hits.totalHits +" / expected: "+ expected +"]", expected, hits.totalHits);
      searcher.close();
      reader.close();
      idxSystem.returnIndexReaders(readers);
    }
    finally
    {
      memoryProvider.stop();
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  public void testIndexSignature() throws ZoieException, IOException
  {
    File idxDir=getIdxDir();
    ZoieSystem<IndexReader,String> idxSystem=createZoie(idxDir,true);
    idxSystem.start();
    DirectoryManager dirMgr = new DefaultDirectoryManager(idxDir);
    try
    {
      int count=TestData.testdata.length;
      List<DataEvent<String>> list;
      IndexSignature sig;

      list=new ArrayList<DataEvent<String>>(count);
      for (int i=0;i<count/2;++i)
      {
        list.add(new DataEvent<String>(i,TestData.testdata[i]));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      assertEquals("index version mismatch after first flush", (count/2 - 1), sig.getVersion());

      list=new ArrayList<DataEvent<String>>(count);
      for (int i=count/2; i < count; ++i)
      {
        list.add(new DataEvent<String>(i,TestData.testdata[i]));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      assertEquals("index version mismatch after second flush", (count - 1), sig.getVersion());
    }
    catch(ZoieException e)
    {
      throw e;
    }
    finally
    {
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }   
  }

  public void testDocIDMapper()
  {
    long[] uidList = new long[500000];
    long[] qryList = new long[100000];
    int intersection = 10000;
    int del = 5;
    int[] ansList1 = new int[qryList.length];
    int[] ansList2 = new int[qryList.length];
    java.util.Random rand = new java.util.Random(System.currentTimeMillis());
    DocIDMapperImpl mapper = null;

    for(int k = 0; k < 10; k++)
    {
      java.util.HashSet<Long> uidset = new java.util.HashSet<Long>();
      java.util.HashSet<Long> qryset = new java.util.HashSet<Long>();
      long id;
      for(int i = 0; i < intersection; i++)
      {
        do { id = (long)rand.nextInt() + (long)(Integer.MAX_VALUE)*2L; } while(id == ZoieIndexReader.DELETED_UID || uidset.contains(id));

        uidset.add(id);
        uidList[i] = (i % del) > 0 ? id : ZoieIndexReader.DELETED_UID;
        qryList[i] = id;
        ansList1[i] = (i % del) > 0 ? i : -1;
      }
      for(int i = intersection; i < uidList.length; i++)
      {
        do { id = (long)rand.nextInt() + (long)(Integer.MAX_VALUE)*2L; } while(id == ZoieIndexReader.DELETED_UID || uidset.contains(id));

        uidset.add(id);
        uidList[i] = (i % del) > 0 ? id : ZoieIndexReader.DELETED_UID;
      }
      for(int i = intersection; i < qryList.length; i++)
      {
        do { id = (long)rand.nextInt() + (long)(Integer.MAX_VALUE)*2L; } while(id == ZoieIndexReader.DELETED_UID || uidset.contains(id) || qryset.contains(id));

        qryset.add(id);
        qryList[i] = id;
        ansList1[i] = -1;
      }

      mapper = new DocIDMapperImpl(uidList);

      for(int i = 0; i < qryList.length; i++)
      {
        ansList2[i] = mapper.getDocID(qryList[i]);
      }

      assertTrue("wrong result", Arrays.equals(ansList1, ansList2));
    }


    //      long time;
    //      for(int k = 0; k < 10; k++)
      //      {
    //        time = System.currentTimeMillis();
    //        for(int j = 0; j < 10; j++)
    //        {
    //          for(int i = 0; i < qryList.length; i++)
    //          {
    //            ansList1[i] = mapper.getDocID(qryList[i]);
    //          }
    //        }
    //        time = System.currentTimeMillis() - time;
    //        System.out.println("ARRAY TIME : " + time);
    //      }
  }

  public void testExportImport() throws ZoieException, IOException
  {
    File idxDir=getIdxDir();
    ZoieSystem<IndexReader,String> idxSystem=createZoie(idxDir,true);
    idxSystem.start();

    DirectoryManager dirMgr = new DefaultDirectoryManager(idxDir);

    String query="zoie";
    QueryParser parser=new QueryParser(Version.LUCENE_CURRENT,"contents",idxSystem.getAnalyzer());
    Query q=null;
    try 
    {
      q=parser.parse(query);
    }
    catch (ParseException e)
    {
      throw new ZoieException(e.getMessage(),e);
    }

    try
    {
      List<DataEvent<String>> list;
      IndexSignature sig;

      list=new ArrayList<DataEvent<String>>(TestData.testdata.length);
      for (int i=0;i<TestData.testdata.length;++i)
      {
        list.add(new DataEvent<String>(i,TestData.testdata[i]));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      assertEquals("index version mismatch after first flush", TestData.testdata.length - 1, sig.getVersion());
      long versionExported = sig.getVersion();

      int hits = countHits(idxSystem, q);

      RandomAccessFile exportFile;
      FileChannel channel;

      exportFile = new RandomAccessFile(new File(getTmpDir(), "zoie_export.dat"), "rw");
      channel = exportFile.getChannel();
      idxSystem.exportSnapshot(channel);
      exportFile.close();
      exportFile = null;
      channel = null;

      list=new ArrayList<DataEvent<String>>(TestData.testdata.length);
      for (int i=0; i < TestData.testdata2.length; ++i)
      {
        list.add(new DataEvent<String>(TestData.testdata.length + i,TestData.testdata2[i]));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      assertEquals("index version mismatch after second flush", TestData.testdata.length + TestData.testdata2.length - 1, sig.getVersion());

      assertEquals("should have no hits", 0, countHits(idxSystem, q));

      exportFile = new RandomAccessFile(new File(getTmpDir(), "zoie_export.dat"), "r");
      channel = exportFile.getChannel();
      idxSystem.importSnapshot(channel);
      exportFile.close();

      assertEquals("count is wrong", hits, countHits(idxSystem, q));

      sig = dirMgr.getCurrentIndexSignature();
      assertEquals("imported version is wrong", versionExported, sig.getVersion());
    }
    catch(ZoieException e)
    {
      throw e;
    }
    finally
    {
      idxSystem.shutdown();
      deleteDirectory(idxDir);
    }
  }

  public void testUIDDocIdSet() throws IOException{
    LongOpenHashSet uidset = new LongOpenHashSet();
    int count = 100;
    Random rand = new Random();
    int id;
    for (int i=0;i<count;++i){
      do { id = rand.nextInt(); } while(id == ZoieIndexReader.DELETED_UID || uidset.contains(id));
      uidset.add(id);
    }

    long[] uidArray = uidset.toLongArray();

    long[] even = new long[uidArray.length/2];
    int[] ans = new int[even.length];
    for (int i=0;i<even.length;++i){
      even[i]=uidArray[i*2];
      ans[i]=i;
    }

    DocIDMapperImpl mapper = new DocIDMapperImpl(even);
    UIDDocIdSet uidSet = new UIDDocIdSet(even, mapper);
    DocIdSetIterator docidIter = uidSet.iterator();
    IntArrayList intList = new IntArrayList();
    int docid;
    while((docid=docidIter.nextDoc())!=DocIdSetIterator.NO_MORE_DOCS){
      intList.add(docid);
    }
    assertTrue("wrong result from iter", Arrays.equals(ans, intList.toIntArray()));

    long[] newidArray = new long[count];
    for (int i=0;i<count;++i){
      newidArray[i]=i;
    }

    mapper = new DocIDMapperImpl(newidArray);
    uidSet = new UIDDocIdSet(newidArray, mapper);
    docidIter = uidSet.iterator();
    intList = new IntArrayList();
    for (int i=0;i<newidArray.length;++i){
      docid = docidIter.advance(i*10);
      if (docid == DocIdSetIterator.NO_MORE_DOCS) break;
      intList.add(docid);
      docid = docidIter.nextDoc();
      if (docid == DocIdSetIterator.NO_MORE_DOCS) break;
      intList.add(docid);
    }

    int[] answer = new int[]{0,1,10,11,20,21,30,31,40,41,50,51,60,61,70,71,80,81,90,91};
    assertTrue("wrong result from mix of next and skip",Arrays.equals(answer, intList.toIntArray()));
  }
}
