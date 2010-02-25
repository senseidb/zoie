package proj.zoie.perf.indexing;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class WikiDocIndexableInterpreter implements ZoieIndexableInterpreter<ContentDoc> {
	private ThreadLocal<SimpleDateFormat> _formatter = new ThreadLocal<SimpleDateFormat>() {
	      protected SimpleDateFormat initialValue() {
	    	  return new SimpleDateFormat("dd-MMM-yyyy hh:mm:ss.SSS");
		      }   
		    };
		    
	public ZoieIndexable convertAndInterpret(ContentDoc src) {
		return new EnWikiIndexable(src);
	}
	
	private class EnWikiIndexable implements ZoieIndexable{
		private ContentDoc _wikiDoc;
		public EnWikiIndexable(ContentDoc wikiDoc){
			_wikiDoc = wikiDoc;
		}
		
		private Document buildDoc(){
			Document doc = new Document();
			String data = _wikiDoc.getTitle();
			if (data!=null){
			  doc.add(new Field("title",data,Store.NO,Index.ANALYZED));
			}
			data = _wikiDoc.getBody();
			if (data!=null){
			  doc.add(new Field("body",data,Store.NO,Index.ANALYZED));
			}
			Date datedata = _wikiDoc.getDate();
			if (datedata!=null){
			  Field dateField = new Field("date",_formatter.get().format(datedata),Store.NO,Index.NOT_ANALYZED_NO_NORMS);
			
			  dateField.setOmitTermFreqAndPositions(true);
			  doc.add(dateField);
			}
			
			Properties props = _wikiDoc.getProps();
			if (props!=null){
				Set<Entry<Object,Object>> entries = props.entrySet();
				for (Entry<Object,Object> entry : entries){
					String name = (String)entry.getKey();
					String val = (String)entry.getValue();
					if (name!=null && val!=null){
					  Field f = new Field(name,val,Store.NO,Index.NOT_ANALYZED_NO_NORMS);
					  f.setOmitTermFreqAndPositions(true);
					  doc.add(f);
					}
				}
			}
			return doc;
		}
		
		public IndexingReq[] buildIndexingReqs() {
			IndexingReq req = new IndexingReq(buildDoc());
			return new IndexingReq[]{req};
		}

		public long getUID() {
			return _wikiDoc.getID();
		}

		public boolean isDeleted() {
			return false;
		}

		public boolean isSkip() {
			return false;
		}
		
	}

}
