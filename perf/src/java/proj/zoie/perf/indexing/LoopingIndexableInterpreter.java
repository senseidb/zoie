package proj.zoie.perf.indexing;

import org.apache.log4j.Logger;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class LoopingIndexableInterpreter<V> implements ZoieIndexableInterpreter<V>
{
  private static final Logger log = Logger.getLogger(LoopingIndexableInterpreter.class);
  
  protected int maxUID;
  private final ZoieIndexableInterpreter<V> _inner;
  
  public LoopingIndexableInterpreter(ZoieIndexableInterpreter<V> inner,int max)
  {
    maxUID = max;
    _inner = inner;
  }
  
  public ZoieIndexable convertAndInterpret(V src) {
	ZoieIndexable innerIndexable = _inner.convertAndInterpret(src);
	long uid = innerIndexable.getUID();
    uid = uid %= maxUID;
    return new LocalWrapperIndexable(uid, innerIndexable);
  }
  
  private static class LocalWrapperIndexable implements ZoieIndexable{
	private final ZoieIndexable _inner;
	private final long _uid;
	
	LocalWrapperIndexable(long uid,ZoieIndexable inner){
		_inner = inner;
		_uid = uid;
	}
	public IndexingReq[] buildIndexingReqs() {
		return _inner.buildIndexingReqs();
	}

	public long getUID() {
		return _uid;
	}

	public boolean isDeleted() {
		return _inner.isDeleted();
	}

	public boolean isSkip() {
		return _inner.isSkip();
	}
	  
  }
}
