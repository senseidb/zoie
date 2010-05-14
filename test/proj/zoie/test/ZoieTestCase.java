package proj.zoie.test;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.impl.InRangeDocIDMapperFactory;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.TestDataInterpreter;
import proj.zoie.test.data.TestInRangeDataInterpreter;
import junit.framework.TestCase;

public class ZoieTestCase extends TestCase
{
  static Logger log = Logger.getLogger(ZoieTestCase.class);
  ZoieTestCase()
  {
    super();
    String confdir = System.getProperty("conf.dir");
    org.apache.log4j.PropertyConfigurator.configure(confdir+"/log4j.properties");
  }

  ZoieTestCase(String name)
  {
    super(name);
    String confdir = System.getProperty("conf.dir");
    org.apache.log4j.PropertyConfigurator.configure(confdir+"/log4j.properties");
  }

  @Override
  public void setUp()
  {
    System.out.println("executing test case: " + getName());
    log.info("\n\n\nexecuting test case: " + getName());
  }
  @Override
  public void tearDown()
  {
    deleteDirectory(getIdxDir());
  }
  protected static File getIdxDir()
  {
    File tmpDir=new File(System.getProperty("java.io.tmpdir"));
    File tempFile = new File(tmpDir, "test-idx");
    int i = 0;
    while (tempFile.exists())
    {
      if (i>10)
      {
        System.out.println("cannot delete");
        log.info("cannot delete");
        return tempFile;
      }
      System.out.println("deleting " + tempFile);
      log.info("deleting " + tempFile);
      tempFile.delete();
      try
      {
        Thread.sleep(50);
      } catch(Exception e)
      {
        log.error("thread interrupted in sleep in deleting file" + e);
      }
      i++;
    }
    return tempFile;
  }

  protected static File getTmpDir()
  {
    return new File(System.getProperty("java.io.tmpdir"));
  }

  protected static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime)
  {
    return createZoie(idxDir, realtime, 20);
  }

  protected static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime,DocIDMapperFactory docidMapperFactory)
  {
    return createZoie(idxDir, realtime, 20,null,docidMapperFactory);
  }

  protected static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime, long delay)
  {
    return createZoie(idxDir,realtime,delay,null,null);
  }


  protected static class TestIndexReaderDecorator implements IndexReaderDecorator<IndexReader>{
    public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader) throws IOException {
      return indexReader;
    }

    public IndexReader redecorate(IndexReader decorated,ZoieIndexReader<IndexReader> copy,boolean withDeletes) throws IOException {
      return decorated;
    }
  }

  protected static ZoieSystem<IndexReader,String> createZoie(File idxDir,boolean realtime, long delay,Analyzer analyzer,DocIDMapperFactory docidMapperFactory)
  {
    ZoieSystem<IndexReader,String> idxSystem=new ZoieSystem<IndexReader, String>(idxDir,new TestDataInterpreter(delay,analyzer),
        new TestIndexReaderDecorator(),docidMapperFactory,null,null,50,100,realtime);
    return idxSystem;
  }

  protected static ZoieSystem<IndexReader,String> createInRangeZoie(File idxDir,boolean realtime, InRangeDocIDMapperFactory docidMapperFactory)
  {
    ZoieSystem<IndexReader,String> idxSystem=new ZoieSystem<IndexReader, String>(idxDir,new TestInRangeDataInterpreter(20,null),
        new TestIndexReaderDecorator(),docidMapperFactory,null,null,50,100,realtime);
    return idxSystem;
  } 
  protected static boolean deleteDirectory(File path) {
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
  protected class QueryThread extends Thread
  {
    public volatile boolean stop = false;
    public volatile boolean mismatch = false;
    public volatile String message = null;
    public Exception exception = null;
  }

}
