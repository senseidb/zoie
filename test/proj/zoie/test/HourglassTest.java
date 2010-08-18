/**
 * 
 */
package proj.zoie.test;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
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

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.hourglass.api.HourglassIndexable;
import proj.zoie.hourglass.api.HourglassIndexableInterpreter;
import proj.zoie.hourglass.impl.HourGlassScheduler;
import proj.zoie.hourglass.impl.Hourglass;
import proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.api.DefaultZoieVersion;
import proj.zoie.api.DefaultZoieVersion.DefaultZoieVersionFactory;

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
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    HierarchyDynamicMBean hdm = new HierarchyDynamicMBean();
    String LOG4J_HIEARCHY_DEFAULT = "log4j:hiearchy=default";
    try
    {
      mbeanServer.registerMBean(hdm, new ObjectName("HouseGlass:name=log4j"));
      // Add the root logger to the Hierarchy MBean
      hdm.addLoggerMBean(Logger.getRootLogger().getName());

      // Get each logger from the Log4J Repository and add it to
      // the Hierarchy MBean created above.
      LoggerRepository r = LogManager.getLoggerRepository();

      java.util.Enumeration loggers = r.getCurrentLoggers();

      int count = 1;
      while (loggers.hasMoreElements())
      {
        String name = ((Logger) loggers.nextElement()).getName();
        if (log.isDebugEnabled())
        {
          log.debug("[contextInitialized]: Registering " + name);
        }
        hdm.addLoggerMBean(name);
        count++;
      }
      if (log.isInfoEnabled())
      {
        log
        .info("[contextInitialized]: " + count
            + " log4j MBeans registered.");
      }
    } catch (Exception e)
    {
      log.error("[contextInitialized]: Exception catched: ", e);
    }
    String schedule = "07 15 20";
    long numTestContent = 20250;
    oneTest(idxDir, schedule, numTestContent); // test starting from empty index
    oneTest(idxDir, schedule, numTestContent); // test index pick up
    return;
  }
  private void oneTest(File idxDir, String schedule, long numTestContent) throws IOException, InterruptedException
  {
    DefaultZoieVersionFactory defaultZoieVersionFactory = new DefaultZoieVersionFactory();
    HourglassDirectoryManagerFactory<DefaultZoieVersion> factory = new HourglassDirectoryManagerFactory<DefaultZoieVersion>(idxDir, new HourGlassScheduler(HourGlassScheduler.FREQUENCY.MINUTELY, schedule), defaultZoieVersionFactory);
    ZoieConfig<DefaultZoieVersion> zConfig = new ZoieConfig<DefaultZoieVersion>(defaultZoieVersionFactory);
    zConfig.setBatchSize(3);
    zConfig.setBatchDelay(10);
    Hourglass<IndexReader, String,DefaultZoieVersion> hourglass = new Hourglass<IndexReader, String,DefaultZoieVersion>(factory, new HourglassTestInterpreter(), new IndexReaderDecorator<IndexReader>(){

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
    MemoryStreamDataProvider<String, DefaultZoieVersion> memoryProvider=new MemoryStreamDataProvider<String, DefaultZoieVersion>();
    memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
    memoryProvider.setDataConsumer(hourglass);
    memoryProvider.start();
    int initNumDocs = getTotalNumDocs(hourglass);
    System.out.println("initial number of DOCs: " + initNumDocs);
    
    long accumulatedTime = 0;
    for(int i=initNumDocs; i<initNumDocs + numTestContent; i++)
    {
      List<DataEvent<String,DefaultZoieVersion>> list=new ArrayList<DataEvent<String,DefaultZoieVersion>>(2);
      DefaultZoieVersion dzv = new DefaultZoieVersion();
      dzv.setVersionId(i);
      list.add(new DataEvent<String,DefaultZoieVersion>("" +i, dzv));
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
  }
  private int getTotalNumDocs(Hourglass<IndexReader, String,DefaultZoieVersion> hourglass)
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
    protected static long nextUID = System.currentTimeMillis();
    public final long UID;
    final String _str;
    public TestHourglassIndexable(String str)
    {
      UID = getNextUID();
      _str = str;
    }
    public static final synchronized long getNextUID()
    {
      return nextUID++;
    }
    public final long getUID()
    {
      return UID;
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
