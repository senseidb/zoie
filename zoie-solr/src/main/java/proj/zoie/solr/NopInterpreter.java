package proj.zoie.solr;

import java.io.Serializable;

import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

public class NopInterpreter<V, VALUE extends Serializable> extends AbstractZoieIndexableInterpreter<V, VALUE>
{

	@Override
	public ZoieIndexable<VALUE> convertAndInterpret(V src) {
		return new ZoieIndexable<VALUE>(){

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

      @Override
      public VALUE getStoreValue()
      {
        return null;
      }

      @Override
      public boolean hasStoreData()
      {
        return false;
      }
			
		};
	}

}
