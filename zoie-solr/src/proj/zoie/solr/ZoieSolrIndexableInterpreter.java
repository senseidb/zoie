package proj.zoie.solr;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

public class ZoieSolrIndexableInterpreter extends
		AbstractZoieIndexableInterpreter<DocumentWithID> {

	@Override
	public ZoieIndexable convertAndInterpret(DocumentWithID src) {
		return src;
	}

}
