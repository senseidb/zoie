/**
 * 
 */
package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
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
import org.apache.lucene.store.Directory;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.hourglass.api.HourglassIndexable;
import proj.zoie.hourglass.api.HourglassIndexableInterpreter;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class HourglassTest extends ZoieTestCase
{
  static Logger log = Logger.getLogger(HourglassTest.class);
  public HourglassTest()
  {
    
  }
  public HourglassTest(String name)
  {
    super(name);
  }
  public void testHourglassDirectoryManagerFactory() throws IOException, InterruptedException, ZoieException
  {
    File idxDir = getIdxDir();
    HourglassDirectoryManagerFactory factory = new HourglassDirectoryManagerFactory(idxDir, 10000);
    ZoieConfig zConfig = new ZoieConfig();
    zConfig.setBatchSize(3);
    zConfig.setBatchDelay(10);
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory, new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>(){

      public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader)
          throws IOException
      {
        return indexReader;
      }

      public IndexReader redecorate(IndexReader decorated,
          ZoieIndexReader<IndexReader> copy, boolean withDeletes) throws IOException
      {
        // TODO Auto-generated method stub
        return decorated;
      }

      public void setDeleteSet(IndexReader reader, DocIdSet docIds)
      {
        // do nothing
      }}, zConfig);
    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();
    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);
    
    long numTestContent = 20250;
    long accumulatedTime = 0;
    for(int i=initNumDocs; i<initNumDocs + numTestContent; i++)
    {
      List<DataEvent<String>> list=new ArrayList<DataEvent<String>>(2);
      list.add(new DataEvent<String>(i,"" +i));
      memoryProvider.addEvents(list);
      if (i%113 !=0) continue;
      long flushtime = System.currentTimeMillis();
      int numDoc = -1;
      List<ZoieIndexReader<IndexReader>> readers=null;
      IndexReader reader = null;
      Searcher searcher = null;
      int oldNum = -1;
      while(numDoc < i + 1)
      {
        if (reader!=null && readers!=null)
        {
          searcher.close();
          searcher = null;
          reader.close();
          hourglass.returnIndexReaders(readers);
        }
        readers = hourglass.getIndexReaders();
        reader = new MultiReader(readers.toArray(new IndexReader[0]),false);
        searcher = new IndexSearcher(reader);
        TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
        numDoc = hitsall.totalHits;
        if (numDoc!=oldNum)System.out.println("numDoc: " + numDoc);
        oldNum = numDoc;
        Thread.sleep(30);
      }
      accumulatedTime += (System.currentTimeMillis() - flushtime);
      TopDocs hits = searcher.search(new TermQuery(new Term("contents",""+i)), 10);
      TopDocs hitsall = searcher.search(new MatchAllDocsQuery(), 10);
      try
      {
        assertEquals("one hit for " + i, 1, hits.totalHits);
        assertEquals("MatchAllDocsHit ", i+1, hitsall.totalHits);
      } finally
      {
        searcher.close();
        searcher = null;
        reader.close();
        reader = null;
        hourglass.returnIndexReaders(readers);
        readers = null;
      }
      System.out.println(((i-initNumDocs)*100/numTestContent) + "%");
    }
    hourglass.shutdown();
    return;
  }
  private int getTotalNumDocs(Hourglass<IndexReader, String> hourglass)
  {
    int numDocs = 0;
    List<ZoieIndexReader<IndexReader>> readers = null;
    try
    {
      readers = hourglass.getIndexReaders();
      for(ZoieIndexReader<IndexReader> reader : readers)
      {
        numDocs += reader.numDocs();
      }
    } catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally
    {
      if (readers != null)
        hourglass.returnIndexReaders(readers);
    }
    return numDocs;
  }
  public static class TestHourglassIndexable extends HourglassIndexable
  {
    final String _str;
    public TestHourglassIndexable(String str)
    {
      _str = str;
    }
    public Document buildDocument(){
      Document doc=new Document();
      doc.add(new Field("contents",_str,Store.YES,Index.ANALYZED));
      try
      {
        Thread.sleep(25); // slow down indexing process
      }
      catch (InterruptedException e)
      {
      }
      return doc;
    }
    
    public IndexingReq[] buildIndexingReqs(){
      return new IndexingReq[]{new IndexingReq(buildDocument(),null)};
    }
    
    public boolean isSkip()
    {
      return false;
    }
    
  }
  public static class HourglassTestInterpreter implements HourglassIndexableInterpreter<String>
  {

    public HourglassIndexable convertAndInterpret(String src)
    {
      log.info("converting " + src);
      return new TestHourglassIndexable(src);
    }
    
  }
}
