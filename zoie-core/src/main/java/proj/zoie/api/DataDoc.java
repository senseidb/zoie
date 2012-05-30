package proj.zoie.api;

import org.apache.lucene.document.Document;

import proj.zoie.api.indexing.AbstractZoieIndexable;

public class DataDoc extends AbstractZoieIndexable {
	private long uid;
	private Document doc;
	private boolean valid;
	public DataDoc(long uid, Document doc) {
		this.uid = uid;
		this.doc = doc;
		this.valid = true;
	}
	
	public DataDoc(long uid) {
		this.uid = uid;
		this.valid = false;
	}
	
	@Override
	public IndexingReq[] buildIndexingReqs() {
		return new IndexingReq[]{new IndexingReq(doc)};
	}

	@Override
	public long getUID() {
		return uid;
	}

	@Override
	public boolean isDeleted() {
		return !valid;
	}

}
