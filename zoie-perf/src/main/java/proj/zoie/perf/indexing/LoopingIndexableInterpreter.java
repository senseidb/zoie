package proj.zoie.perf.indexing;

import java.io.Serializable;

import org.apache.log4j.Logger;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class LoopingIndexableInterpreter<V, VALUE extends Serializable> implements ZoieIndexableInterpreter<V, VALUE>
{
  private static final Logger log = Logger.getLogger(LoopingIndexableInterpreter.class);
  
  protected int maxUID;
  private final ZoieIndexableInterpreter<V, VALUE> _inner;
  
  public LoopingIndexableInterpreter(ZoieIndexableInterpreter<V, VALUE> inner,int max)
  {
    maxUID = max;
    _inner = inner;
  }
  
  public ZoieIndexable<VALUE> convertAndInterpret(V src) {
	ZoieIndexable<VALUE> innerIndexable = _inner.convertAndInterpret(src);
	long uid = innerIndexable.getUID();
    uid = uid %= maxUID;
    return new LocalWrapperIndexable<VALUE>(uid, innerIndexable);
  }
  
  private static class LocalWrapperIndexable<VALUE extends Serializable> implements ZoieIndexable<VALUE>{
	private final ZoieIndexable<VALUE> _inner;
	private final long _uid;
	
	LocalWrapperIndexable(long uid,ZoieIndexable<VALUE> inner){
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
  @Override
  public VALUE getStoreValue()
  {
    return _inner.getStoreValue();
  }
  @Override
  public boolean hasStoreData()
  {
    return _inner.hasStoreData();
  }
	  
  }
}
