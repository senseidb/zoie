package proj.zoie.test.data;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class TestDataInterpreter implements ZoieIndexableInterpreter<String> {

    long _delay;
    final Analyzer _analyzer;
    
    public TestDataInterpreter()
    {
      this(0,null);
    }
    
    public TestDataInterpreter(long delay)
    {
      this(delay,null);
    }
    
    public TestDataInterpreter(long delay,Analyzer analyzer)
    {
      _delay = delay;
      _analyzer = analyzer;
    }
    
	public ZoieIndexable interpret(final String src) {
		String[] parts=src.split(" ");
		final long id=Long.parseLong(parts[parts.length-1])+((long)(Integer.MAX_VALUE)*2L);
		return new ZoieIndexable(){
			public Document buildDocument(){
				Document doc=new Document();
				doc.add(new Field("contents",src,Store.NO,Index.ANALYZED));
				doc.add(new Field("id",String.valueOf(id),Store.YES,Index.NO));
				try
                {
                  Thread.sleep(_delay); // slow down indexing process
                }
                catch (InterruptedException e)
                {
                }
                return doc;
			}
			
			public IndexingReq[] buildIndexingReqs(){
				return new IndexingReq[]{new IndexingReq(buildDocument(),getAnalyzer())};
			}
			
			public Analyzer getAnalyzer(){
				return id%2 == 0 ? null : _analyzer;
			}

			public long getUID() {
				return id;
			}

			public boolean isDeleted() {
				return false;
			}

			public boolean isSkip() {
				return false;
			}
		};
	}

	public ZoieIndexable convertAndInterpret(String src) {
		return interpret(src);
	}

}
