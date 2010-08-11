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
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
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
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.UIDDocIdSet;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DocIDMapper.DocIDArray;

import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;
import proj.zoie.api.impl.DocIDMapperImpl;
import proj.zoie.api.impl.InRangeDocIDMapperFactory;
import proj.zoie.impl.indexing.AsyncDataConsumer;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.test.data.TestData;
import proj.zoie.test.mock.MockDataLoader;


public class ZoieTest extends ZoieTestCase
{
  static Logger log = Logger.getLogger(ZoieTest.class);
  
  public ZoieTest() {
  }

  public ZoieTest(String name) {
    super(name);
  }

  private static int countHits(ZoieSystem<IndexReader,String, DefaultZoieVersion> idxSystem, Query q) throws IOException
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
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem=createZoie(idxDir,true,20,new WhitespaceAnalyzer(),null,defaultZoieVersionFactory);
    idxSystem.start();

    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();

    
    List<DataEvent<String, DefaultZoieVersion>> list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(2);
    
    DefaultZoieVersion zvt1 = new DefaultZoieVersion();
    zvt1.setVersionId(0);
    
    DefaultZoieVersion zvt2 = new DefaultZoieVersion();
    zvt2.setVersionId(1);
  
    list.add(new DataEvent<String,DefaultZoieVersion>("hao,yan 0", zvt1));
    list.add(new DataEvent<String,DefaultZoieVersion>("hao,yan 1", zvt2));
    memoryProvider.addEvents(list);

    memoryProvider.flush();
    idxSystem.syncWthVersion(10000, zvt2);
    List<ZoieIndexReader<IndexReader>> readers = null;
    Searcher searcher =null;
    MultiReader reader = null;
    try
    {     
      readers=idxSystem.getIndexReaders();
      reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]),false);
      for(int i=0; i<readers.size(); i++)
      {
        IndexReader ir = readers.get(i);
        //Map<String, String> commitData = reader.getCommitUserData(ir.directory()); // = new HashMap<String,String>();
        //System.out.println("ZoieTest: directory: " + ir.directory());
        //System.out.println("ZoieTest: commitData(ZoieVersion): " + commitData);  
      }
      //Map<String, String> commitData = reader.getCommitUserData(reader.directory());// = new HashMap<String,String>();
//      for(int i=0; i<readers.size(); i++)
//      {
//        IndexReader ir = readers.get(i);
//        Map<String, String> commitData = IndexReader.getCommitUserData(ir.directory()); // = new HashMap<String,String>();
//        System.out.println("i:" + i + "ZoieTest: directory: " + ir.directory());
//        System.out.println("i:" + i + "ZoieTest: commitData(ZoieVersion): " + commitData);  
//      }
     
      //Map<String, String> commitData = IndexReader.getCommitUserData(reader.directory());// = new HashMap<String,String>();
      //System.out.println("ZoieTest:commitData" + commitData);  
     
      //commitData = reader.getCommitUserData();      
      
//int x = reader.maxDoc();
//for(int y = 0; y<x; y++)
//{
//  Document d = reader.document(y);
//  System.out.println(d.toString());  
//}

//TermEnum terms = reader.terms();
//while(terms.next())
//{
//  System.out.println(terms.term().text());
//  TermDocs td = reader.termDocs(terms.term());
//  while(td.next())
//  {
//    System.out.println(td.doc());
//  }
//}

//terms = reader.terms();
//while(terms.next())
//{
//  System.out.println("term:" + terms.term().text());
//  TermPositions tp = reader.termPositions(terms.term());
//  
//  while(tp.next())
//  {
//    System.out.println("docID: " + tp.doc() + "freq: " + tp.freq());
//    System.out.println("positions");
//    for(int i=0; i<tp.freq();i++)
//    {
//      System.out.println(tp.nextPosition());
//    }
//  }
//}
      searcher=new IndexSearcher(reader);

      TopDocs hits=searcher.search(new TermQuery(new Term("contents","hao,yan")),10);

      assertEquals(1,hits.totalHits);
      //assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));
     assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L+1L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));

      hits=searcher.search(new TermQuery(new Term("contents","hao")),10);
      assertEquals(1,hits.totalHits);
      //assertEquals(String.valueOf((long)((long)Integer.MAX_VALUE*2L)),searcher.doc(hits.scoreDocs[0].doc).get("id"));
      System.out.println("hao: searching is successful");
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
  
  public void testRealtime2() throws ZoieException{
	  File idxDir=getIdxDir();
	  DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
	  ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem=createZoie(idxDir,true,defaultZoieVersionFactory);
	  idxSystem.start();
	    
	  MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
	  memoryProvider.setDataConsumer(idxSystem);
	  memoryProvider.start();
	  
	  try{
	    int count=TestData.testdata.length;
	    List<DataEvent<String,DefaultZoieVersion>> list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
	    DefaultZoieVersion zvt = null;
	    for (int i=0;i<count;++i){
	      zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
	      list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i], zvt));
	    }
	    memoryProvider.addEvents(list);
	    memoryProvider.flush();
	    
	    idxSystem.flushEvents(1000);
	    
	    List<ZoieIndexReader<IndexReader>> readers = idxSystem.getIndexReaders();
	      
	    int numDocs = 0;
	    for (ZoieIndexReader<IndexReader> r : readers){
	      numDocs += r.numDocs();
	    }
	    idxSystem.returnIndexReaders(readers);
	    
	    assertEquals(count, numDocs);
	  }
	  catch(IOException ioe){
        throw new ZoieException(ioe.getMessage());
      }
      finally
      {
        memoryProvider.stop();
        idxSystem.shutdown();
        deleteDirectory(idxDir);
      }	
  }

  // hao: test for new zoieVersion
  public void testRealtime() throws ZoieException
  {
    File idxDir=getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem=createZoie(idxDir,true,defaultZoieVersionFactory);
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
    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      int count=TestData.testdata.length;
      List<DataEvent<String,DefaultZoieVersion>> list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
      
      DefaultZoieVersion zvt = null;
      for (int i=0;i<count;++i)
      {
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i], zvt));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, zvt);

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

//          for(int j=0; j<readers.size(); j++)
//          {
//            IndexReader ir = readers.get(j);
//            Map<String, String> commitData = IndexReader.getCommitUserData(ir.directory()); // = new HashMap<String,String>();
//            System.out.println("j:" + j + "ZoieTest: directory: " + ir.directory());
//            System.out.println("j:" + j + "ZoieTest: commitData(ZoieVersion): " + commitData);  
//          }
          
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
    MockDataLoader<Integer, DefaultZoieVersion> consumer=new MockDataLoader<Integer, DefaultZoieVersion>();
    MemoryStreamDataProvider<Integer, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<Integer, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(consumer);
    memoryProvider.start();
    try
    {
      int count=10;

      List<DataEvent<Integer,DefaultZoieVersion>> list=new ArrayList<DataEvent<Integer,DefaultZoieVersion>>(count);
      DefaultZoieVersion zvt = null;
      for (int i=0;i<count;++i)
      {
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        //list.add(new DataEvent<Integer,DefaultZoieVersion>(i,i));
        list.add(new DataEvent<Integer,DefaultZoieVersion>(i,zvt));
      }
      memoryProvider.addEvents(list);

      //memoryProvider.syncWthVersion(10000, count-1);
      memoryProvider.syncWthVersion(10000, zvt);
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
    //final long[] delays = {100L, 1000L };
    final int[] batchSizes = { 1, 10, 100, 1000, 1000 };
    final int count=1000; 
    //final int count=TestData.testdata.length;
    final long timeout = 10000L;

    for(long delay : delays)
    {
      for(int batchSize : batchSizes)
      {

        if(delay * (count / batchSize + 1) > timeout)
        {
          continue; // skip this combination. it will take too long.
        }

        MockDataLoader<Integer,DefaultZoieVersion> mockLoader=new MockDataLoader<Integer,DefaultZoieVersion>();
        mockLoader.setDelay(delay);

        AsyncDataConsumer<Integer,DefaultZoieVersion> asyncConsumer = new AsyncDataConsumer<Integer,DefaultZoieVersion>();
        asyncConsumer.setDataConsumer(mockLoader);
        asyncConsumer.setBatchSize(batchSize);
        asyncConsumer.start();

        MemoryStreamDataProvider<Integer, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<Integer, DefaultZoieVersion>();
        memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
        
        memoryProvider.setDataConsumer(asyncConsumer);
        memoryProvider.start();
        memoryProvider.setBatchSize(batchSize);
        try
        {
          List<DataEvent<Integer,DefaultZoieVersion>> list=new ArrayList<DataEvent<Integer,DefaultZoieVersion>>(count);
          DefaultZoieVersion zvt = null;
          for (int i=0;i<count;++i)
          {
            //System.out.println("ZoieTest.java:testAsyncDataConsumer():delay:" + delay + ", batchSize:" + batchSize + ",forloop: i: " + i);
            zvt = new DefaultZoieVersion();
            zvt.setVersionId(i);
            list.add(new DataEvent<Integer,DefaultZoieVersion>(i,zvt));
          }
          memoryProvider.addEvents(list);
          //System.out.println("ZoieTest.java:testAsyncDataConsumer():before SyncWthVersion, zvt.id:" + zvt.getVersionId());
          //asyncConsumer.syncWthVersion(timeout, (long)(count-1));
          asyncConsumer.syncWthVersion(timeout, zvt);
          
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

  public void testDelSet() throws ZoieException
  {
    for(int i=0; i<10; i++)
    {
      System.out.println("testDelSet Round: " + i);
      log.info("\n\n\ntestDelSet Round: " + i);
      testDelSetImpl();
    }
  }

  private void testDelSetImpl() throws ZoieException
  {
    File idxDir = getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    final ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem = createZoie(idxDir,true, 100,defaultZoieVersionFactory);
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
                log.info(sb.toString());
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
                log.error(ioe.getMessage(),ioe);
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
      queryThreads[i].setDaemon(true);
    }

    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      idxSystem.setBatchSize(10);

      final int count = TestData.testdata.length;
      List<DataEvent<String,DefaultZoieVersion>> list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
      DefaultZoieVersion zvt = null;
      for (int i = 0; i < count; i++)
      {
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        //list.add(new DataEvent<String,DefaultZoieVersion>(i, TestData.testdata[i]));
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i], zvt));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(100000, zvt);

      for(QueryThread queryThread : queryThreads) queryThread.start();

      for(int n = 1; n <= 3; n++)
      {
        for (int i = 0; i < count; i++)
        {
          zvt = new DefaultZoieVersion();
          long version = n * count + i;
          zvt.setVersionId(version);
          list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(1);
          list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i], zvt));
          memoryProvider.addEvents(list);
          idxSystem.syncWthVersion(100000, zvt);
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
    log.info(" done round");
    for(QueryThread queryThread : queryThreads)
    {
      if(queryThread.exception != null) throw new ZoieException(queryThread.exception);
    }
  }

  public void testUpdates() throws ZoieException, ParseException, IOException
  {
    File idxDir = getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    final ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem = createZoie(idxDir,true,defaultZoieVersionFactory);
    idxSystem.start();
    final String query = "zoie";

    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    try
    {
      idxSystem.setBatchSize(10);

      long version = 0;
      DefaultZoieVersion zvt = null;
      final int count = TestData.testdata.length;
      List<DataEvent<String,DefaultZoieVersion>> list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
      for (int i = 0; i < count; i++)
      {
        //version = i;
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        //list.add(new DataEvent<String>(i, TestData.testdata[i]));
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i],zvt));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, zvt);

     
      
      QueryParser parser = new QueryParser(Version.LUCENE_CURRENT,"contents",idxSystem.getAnalyzer());
      Query q;
      Searcher searcher = null;
      List<ZoieIndexReader<IndexReader>> readers = null;

      TopDocs hits;

      q = parser.parse("zoie");
      readers=idxSystem.getIndexReaders();
      
      for(int i=0; i<readers.size(); i++)
      {
        IndexReader ir = readers.get(i);
        Map<String, String> commitData = IndexReader.getCommitUserData(ir.directory()); // = new HashMap<String,String>();
        System.out.println("i:" + i + "ZoieTest: directory: " + ir.directory());
        System.out.println("i:" + i + "ZoieTest: commitData(ZoieVersion): " + commitData);  
      }
      
      if(2>1) return;
      
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

      list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(TestData.testdata2.length);
      for(int i = 0; i < TestData.testdata2.length; i++)
      {
        version = count + i;
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(version);
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata2[i],zvt));
      }
      memoryProvider.addEvents(list);
      idxSystem.syncWthVersion(10000, zvt);

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
      //deleteDirectory(idxDir);
    }
  }

  public void testIndexSignature() throws ZoieException, IOException
  {
    File idxDir=getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem=createZoie(idxDir,true,defaultZoieVersionFactory);
    idxSystem.start();
    DefaultDirectoryManager<DefaultZoieVersion> dirMgr = new DefaultDirectoryManager<DefaultZoieVersion>(idxDir,defaultZoieVersionFactory);
    try
    {
      int count=TestData.testdata.length;
      List<DataEvent<String,DefaultZoieVersion>> list;
      IndexSignature<DefaultZoieVersion> sig;

      list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
      for (int i=0;i<count/2;++i)
      {
        DefaultZoieVersion zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i], zvt));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      DefaultZoieVersion dzv = (DefaultZoieVersion)(sig.getVersion());
      assertEquals("index version mismatch after first flush", (count/2 - 1), dzv.getVersionId());

      list=new ArrayList<DataEvent<String, DefaultZoieVersion>>(count);
      for (int i=count/2; i < count; ++i)
      {
        DefaultZoieVersion zvt = new DefaultZoieVersion();
        zvt.setVersionId(i);
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i],zvt));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      sig = dirMgr.getCurrentIndexSignature();

      dzv = (DefaultZoieVersion)(sig.getVersion());
      System.out.println("count: "+ count);
      assertEquals("index version mismatch after second flush", (count - 1), dzv.getVersionId());
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
  
  public void testDocIDMapperFactory() throws Exception{ 

    File idxDir=getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader,String, DefaultZoieVersion> idxSystem=createZoie(idxDir,true,new InRangeDocIDMapperFactory(0, 1000000),defaultZoieVersionFactory);
    idxSystem.start();
    int numDiskIdx = 0;
    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    idxSystem.setBatchSize(10);

    //long version = 0;
    final int count = TestData.testdata.length;
    List<DataEvent<String, DefaultZoieVersion>> list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
    DefaultZoieVersion zvt = null;
    for (int i = 0; i < count; i++){
      zvt = new DefaultZoieVersion();
      zvt.setVersionId(i);
      //version = i;
      list.add(new DataEvent<String, DefaultZoieVersion>(TestData.testdata[i], zvt));
    }
    memoryProvider.addEvents(list);
    idxSystem.syncWthVersion(10000, zvt);
  
    List<ZoieIndexReader<IndexReader>> readerList= idxSystem.getIndexReaders();       
    for (ZoieIndexReader<IndexReader> reader : readerList){
      DocIDMapper mapper = reader.getDocIDMaper();
      if (!(mapper instanceof DocIDMapperImpl)){
        numDiskIdx++; 
      }
    }
    idxSystem.returnIndexReaders(readerList);
    memoryProvider.stop();
    idxSystem.shutdown();
    deleteDirectory(idxDir);
    
    assertTrue(numDiskIdx>0);
  }

  public void testShutdown() throws Exception
  {
    File idxDir = getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader, String, DefaultZoieVersion> idxSystem = createInRangeZoie(idxDir, true, new InRangeDocIDMapperFactory(0, 1000000, 0), defaultZoieVersionFactory);
    idxSystem.start();
    int numDiskIdx = 0;
    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider = new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    idxSystem.setBatchSize(5);
    memoryProvider.stop();
    idxSystem.shutdown();
    deleteDirectory(idxDir);
  }

  public void testInRangeDocIDMapperFactory() throws Exception
  {
    File idxDir = getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader, String, DefaultZoieVersion> idxSystem = createInRangeZoie(idxDir, true, new InRangeDocIDMapperFactory(0, 1000000, 0), defaultZoieVersionFactory);
    idxSystem.start();
    int numDiskIdx = 0;
    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider = new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    idxSystem.setBatchSize(5);

    long version = 0;
    DefaultZoieVersion zvt = null;
    for(int rep = 0 ; rep<5; rep++)
    {
      final int count = TestData.testdata.length;
      List<DataEvent<String,DefaultZoieVersion>> list = new ArrayList<DataEvent<String,DefaultZoieVersion>>(count);
      for (int i = 0; i < count; i++)
      {
        version = i + rep*count;
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(version);
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i],zvt));
      }
      memoryProvider.addEvents(list);
      idxSystem.flushEvents(10000);
      idxSystem.syncWthVersion(10000, zvt);
    }
    List<ZoieIndexReader<IndexReader>> readerList = idxSystem.getIndexReaders();
    // test UIDs from TestInRangeDataInterpreter 0-9
    for(ZoieIndexReader<IndexReader> reader : readerList)
    {
      int maxDoc = reader.maxDoc();
      DocIDMapper gmapper = reader.getDocIDMaper();
      ZoieIndexReader[] readers = gmapper.getSubReaders();
      log.info(Arrays.toString(readers));
      int[] starts = gmapper.getStarts();
      for(long uid = 0; uid<10; uid++)
      {
        int readeridx = gmapper.getReaderIndex(uid);
        if (readeridx<0 || reader.isDeleted(gmapper.getDocID(uid)))
        {
          continue;
        }
        log.info(
            "uid: " + uid + " global:" +gmapper.getDocID(uid)+ " start: " + starts[readeridx] +
            " local:" + readers[readeridx].getDocIDMaper().getDocID(uid) +
            "?=" + (gmapper.getDocID(uid)-starts[readeridx])+
            (reader.isDeleted(gmapper.getDocID(uid))?"deleted":""));
        assertTrue("", (gmapper.getDocID(uid)-starts[readeridx]) == readers[readeridx].getDocIDMaper().getDocID(uid));
      }
    }
    for(ZoieIndexReader<IndexReader> reader : readerList)
    {
      DocIDMapper mapper = reader.getDocIDMaper();
      log.info(mapper);
      if (!(mapper instanceof DocIDMapperImpl))
      {
        numDiskIdx++;
      }
    }
    idxSystem.returnIndexReaders(readerList);
    memoryProvider.stop();
    idxSystem.shutdown();
    deleteDirectory(idxDir);

    assertTrue(numDiskIdx > 0);
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
      DocIDArray result = mapper.getDocIDArray(qryList);
      int[] resarr = result.docids;
      for(int i = 0; i < qryList.length; i++)
      {
        assertEquals("wrong result", ansList2[i], resarr[i]);
      }
      result.close();
    }

  }

  public void testExportImport() throws ZoieException, IOException
  {
    File idxDir=getIdxDir();
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    ZoieSystem<IndexReader,String,DefaultZoieVersion> idxSystem=createZoie(idxDir,true,defaultZoieVersionFactory);
    idxSystem.start();

    DirectoryManager dirMgr = new DefaultDirectoryManager(idxDir,defaultZoieVersionFactory);

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
      List<DataEvent<String,DefaultZoieVersion>> list;
      DefaultZoieVersion zvt = null;

      list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(TestData.testdata.length);
      for (int i=0;i<TestData.testdata.length;++i)
      {
        zvt = new DefaultZoieVersion();        
        zvt.setVersionId(i);
        list.add(new DataEvent<String,DefaultZoieVersion>(TestData.testdata[i],zvt));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      
      DefaultZoieVersion versionExported = new DefaultZoieVersion();
      versionExported.setVersionId(zvt.getVersionId());
          
      assertEquals("index version mismatch after first flush", TestData.testdata.length - 1, zvt.getVersionId());
      
      
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
     
      list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(TestData.testdata2.length);
      for (int i=0; i < TestData.testdata2.length; ++i)
      {
        zvt = new DefaultZoieVersion();
        zvt.setVersionId(TestData.testdata.length+i);
        
        list.add(new DataEvent<String, DefaultZoieVersion>(TestData.testdata.length + TestData.testdata2[i],zvt));
      }
      idxSystem.consume(list);
      idxSystem.flushEvents(100000);
      zvt = (DefaultZoieVersion)dirMgr.getVersion();
      assertEquals("index version mismatch after second flush", TestData.testdata.length + TestData.testdata2.length - 1, zvt.getVersionId());
      assertEquals("should have no hits", 0, countHits(idxSystem, q));
      
      exportFile = new RandomAccessFile(new File(getTmpDir(), "zoie_export.dat"), "r");
      channel = exportFile.getChannel();
      idxSystem.importSnapshot(channel);
      channel.close();
      exportFile.close();

      assertEquals("count is wrong", hits, countHits(idxSystem, q));

      
      zvt = (DefaultZoieVersion)dirMgr.getVersion();      
      assertEquals("imported version is wrong", versionExported.getVersionId(), zvt.getVersionId());    }
     
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
