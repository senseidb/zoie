package proj.zoie.api.indexing;

public abstract class AbstractZoieIndexableInterpreter<V> implements
		ZoieIndexableInterpreter<V> {
	public abstract ZoieIndexable convertAndInterpret(V src);
}
