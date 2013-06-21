package proj.zoie.perf.indexing;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

public class TweetInterpreter extends AbstractZoieIndexableInterpreter<String> {
  
  private static long id = 0; 
  @Override
  public ZoieIndexable convertAndInterpret(String tweet) {
    try {
      final String text = tweet;
      final long uid = id++;
      return new AbstractZoieIndexable() {

        @Override
        public IndexingReq[] buildIndexingReqs() {
          Document doc = new Document();
          doc.add(new Field("contents", text, Store.NO, Index.ANALYZED));
          return new IndexingReq[] { new IndexingReq(doc) };
        }

        @Override
        public long getUID() {
          return uid;
        }

        @Override
        public boolean isDeleted() {
          return false;
        }

        @Override
        public boolean isSkip() {
          return false;
        }

      };
    } catch (Exception e) {
      return new AbstractZoieIndexable() {

        @Override
        public IndexingReq[] buildIndexingReqs() {
          return null;
        }

        @Override
        public long getUID() {
          return 0;
        }

        @Override
        public boolean isDeleted() {
          return false;
        }

        @Override
        public boolean isSkip() {
          return true;
        }

      };
    }
  }

}
