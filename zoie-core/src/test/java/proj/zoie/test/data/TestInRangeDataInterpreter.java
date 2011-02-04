/**
 * 
 */
package proj.zoie.test.data;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 */
public class TestInRangeDataInterpreter implements
    ZoieIndexableInterpreter<String, String>
{

  long _delay;
  final Analyzer _analyzer;

  public TestInRangeDataInterpreter()
  {
    this(0, null);
  }

  public TestInRangeDataInterpreter(long delay)
  {
    this(delay, null);
  }

  public TestInRangeDataInterpreter(long delay, Analyzer analyzer)
  {
    _delay = delay;
    _analyzer = analyzer;
  }

  public ZoieIndexable<String> interpret(final String src)
  {
    String[] parts = src.split(" ");
    final long id = Long.parseLong(parts[parts.length - 1]);
    return new ZoieIndexable<String>()
    {
      public Document buildDocument()
      {
        Document doc = new Document();
        doc.add(new Field("contents", src, Store.NO, Index.ANALYZED));
        doc.add(new Field("id", String.valueOf(id), Store.YES, Index.NO));
        try
        {
          Thread.sleep(_delay); // slow down indexing process
        } catch (InterruptedException e)
        {
        }
        return doc;
      }

      public IndexingReq[] buildIndexingReqs()
      {
        return new IndexingReq[] { new IndexingReq(buildDocument(),
            getAnalyzer()) };
      }

      public Analyzer getAnalyzer()
      {
        return id % 2 == 0 ? null : _analyzer;
      }

      public long getUID()
      {
        return id;
      }

      public boolean isDeleted()
      {
        return false;
      }

      public boolean isSkip()
      {
        return false;
      }

      @Override
      public String getStoreValue()
      {
        return "" + getUID();
      }

      @Override
      public boolean hasStoreData()
      {
        return true;
      }
    };
  }

  public ZoieIndexable<String> convertAndInterpret(String src)
  {
    return interpret(src);
  }

}
