package proj.zoie.solr;

import org.apache.lucene.document.Document;

import proj.zoie.api.indexing.AbstractZoieIndexable;

/**
 * This ZoieIndexable does not have store data.
 */
public class DocumentWithID extends AbstractZoieIndexable {
  private final long _id;
  private final Document _doc;
  private final boolean _isDel;

  public DocumentWithID(long id, Document doc) {
    _id = id;
    _doc = doc;
    _isDel = false;
  }

  public DocumentWithID(long id, boolean delete) {
    _id = id;
    _doc = null;
    _isDel = delete;
  }

  @Override
  public IndexingReq[] buildIndexingReqs() {
    if (_doc != null) {
      return new IndexingReq[] { new IndexingReq(_doc) };
    } else {
      return new IndexingReq[0];
    }
  }

  @Override
  public long getUID() {
    return _id;
  }

  @Override
  public boolean isDeleted() {
    return _isDel;
  }
}
