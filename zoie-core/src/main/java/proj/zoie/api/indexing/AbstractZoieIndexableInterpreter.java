package proj.zoie.api.indexing;


/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 * @param <V> The type of the data to be interpreted.
 */
public abstract class AbstractZoieIndexableInterpreter<V> implements ZoieIndexableInterpreter<V>
{
	public abstract ZoieIndexable convertAndInterpret(V src);
}
