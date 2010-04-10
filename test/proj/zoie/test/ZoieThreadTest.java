package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
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

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieExecutors;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.TestData;

public class ZoieThreadTest extends ZoieTestCase
{
  static Logger log = Logger.getLogger(ZoieThreadTest.class);
  public ZoieThreadTest(){}
  public ZoieThreadTest(String name)
  {
    super(name);
  }

  private static abstract class QueryRunnable implements Runnable
  {
    public volatile boolean stop = false;
    public volatile boolean mismatch = false;
    public volatile String message = null;
    public Exception exception = null;
  }
  public void testThreadDelImpl() throws ZoieException
  {
    File idxDir = getIdxDir();
    final ZoieSystem<IndexReader,String> idxSystem = createZoie(idxDir,true, 100);
    idxSystem.start();
    final String query = "zoie";
    int numThreads = 5;
    QueryRunnable[] queryRunnables = new QueryRunnable[numThreads];
    for(int i = 0; i < queryRunnables.length; i++)
    {
      queryRunnables[i] = new QueryRunnable()
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
    }

    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(idxSystem);
    memoryProvider.start();
    ExecutorService threadPool = ZoieExecutors.newCachedThreadPool();
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
      Future[] futures = new Future<?>[queryRunnables.length];
      for(int x = 0 ; x< queryRunnables.length; x++)
      {
        futures[x] = threadPool.submit(queryRunnables[x]);
      }

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
        for(QueryRunnable queryThread : queryRunnables) stopNow |= queryThread.stop;
        if(stopNow) break;
      }
      for(QueryRunnable queryThread : queryRunnables) queryThread.stop = true; // stop all query threads
      for(int x = 0 ; x< queryRunnables.length; x++)
      {
        futures[x].get();
        assertTrue("count mismatch["+queryRunnables[x].message +"]", !queryRunnables[x].mismatch);
      }
    }
    catch(Exception e)
    {
      for(QueryRunnable queryThread : queryRunnables)
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
    for(QueryRunnable queryThread : queryRunnables)
    {
      if(queryThread.exception != null) throw new ZoieException(queryThread.exception);
    }
  }
}
