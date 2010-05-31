/**
 * 
 */
package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;
import proj.zoie.hourglass.api.HourglassIndexable;
import proj.zoie.hourglass.api.HourglassIndexableInterpreter;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;

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
    DirectoryManager dirMgr = factory.getDirectoryManager();
    Hourglass<IndexReader, String> hourglass = new Hourglass<IndexReader, String>(factory, new HourglassTestInterpreter(), new IndexReaderDecorator(){

      public IndexReader decorate(ZoieIndexReader indexReader)
          throws IOException
      {
        return indexReader;
      }

      public IndexReader redecorate(IndexReader decorated,
          ZoieIndexReader copy, boolean withDeletes) throws IOException
      {
        // TODO Auto-generated method stub
        return decorated;
      }}, null, null, 1, 10);
    MemoryStreamDataProvider<String> memoryProvider=new MemoryStreamDataProvider<String>();
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();
    long numTestContent = 10025;
    long accumulatedTime = 0;
    for(int i=0; i<numTestContent; i++)
    {
      List<DataEvent<String>> list=new ArrayList<DataEvent<String>>(2);
      list.add(new DataEvent<String>(i,"" +i));
      memoryProvider.addEvents(list);
      if (i%113 !=0) continue;
      memoryProvider.flush();
      long flushtime = System.currentTimeMillis();
      int numDoc = -1;
      List<ZoieIndexReader<IndexReader>> readers=null;
      IndexReader reader = null;
      while(numDoc < i + 1)
      {
        if (reader!=null && readers!=null)
        {
          reader.close();
          hourglass.returnIndexReaders(readers);
        }
        readers = hourglass.getIndexReaders();
        reader = new MultiReader(readers.toArray(new IndexReader[0]),false);
        numDoc = reader.numDocs();
//        System.out.println("numDoc " + numDoc);
        Thread.sleep(30);
      }
      System.out.println("time : " + (System.currentTimeMillis() - flushtime));
      List<Directory> dirlist = factory.getAllArchivedDirectories();
//      System.out.println(Arrays.toString(dirlist.toArray(new Directory[0])));
      accumulatedTime += (System.currentTimeMillis() - flushtime);
      Searcher searcher = new IndexSearcher(reader);
      TopDocs hits = searcher.search(new TermQuery(new Term("contents",""+i)), 10);
      assertEquals("one hit for " + i, 1, hits.totalHits);
      reader.close();
      reader = null;
      hourglass.returnIndexReaders(readers);
      readers = null;
    }
    System.out.println("average time: " + ((float)accumulatedTime/(float)numTestContent));
    return;
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
      return new TestHourglassIndexable(src);
    }
    
  }
}
