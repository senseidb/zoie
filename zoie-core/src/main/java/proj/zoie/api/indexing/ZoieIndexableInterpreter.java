package proj.zoie.api.indexing;


/**
 * Interface to translate from a data object to an indexable object.
 */
public interface ZoieIndexableInterpreter<V>{
	ZoieIndexable convertAndInterpret(V src);
}
