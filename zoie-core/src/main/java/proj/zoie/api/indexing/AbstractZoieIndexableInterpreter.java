package proj.zoie.api.indexing;

import java.io.Serializable;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <V> The type of the data to be interpreted.
 * @param <VALUE> The type of the data for the associated Key-Value Data store.
 */
public abstract class AbstractZoieIndexableInterpreter<V, VALUE extends Serializable> implements ZoieIndexableInterpreter<V, VALUE>
{
	public abstract ZoieIndexable<VALUE> convertAndInterpret(V src);
}
