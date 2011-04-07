package proj.zoie.solr;


import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;

public class NopInterpreter<V> extends AbstractZoieIndexableInterpreter<V>
{

	@Override
	public ZoieIndexable convertAndInterpret(V src) {
		return new AbstractZoieIndexable(){

			public IndexingReq[] buildIndexingReqs() {
				return null;
			}

			public long getUID() {
				return 0;
			}

			public boolean isDeleted() {
				return false;
			}

			public boolean isSkip() {
				return false;
			}
		};
	}

}
