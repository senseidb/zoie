package proj.zoie.solr;

import java.io.Serializable;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;

/**
 * Does not support store data.
 */
public class ZoieSolrIndexableInterpreter extends AbstractZoieIndexableInterpreter<DocumentWithID> {

  @Override
  public DocumentWithID convertAndInterpret(DocumentWithID src) {
    return src;
  }

}
