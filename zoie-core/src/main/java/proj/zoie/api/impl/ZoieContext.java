package proj.zoie.api.impl;

import java.util.Map.Entry;
import java.util.WeakHashMap;

import proj.zoie.api.ZoieIndexReader;

/**
 * This class holds the ZoieContext for each thread. Any child thread inherits
 * the context from the parent. The inherited context is a clone of the parent
 * context so that forthcoming changes to the contexts of the parent and the
 * children are independent of each other.
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class ZoieContext {
  private static InheritableThreadLocal<ZoieContext> _context = new InheritableThreadLocal<ZoieContext>() {
    @Override
    protected ZoieContext childValue(ZoieContext parentValue) {
      return parentValue.clone();
    }

    @Override
    protected ZoieContext initialValue() {
      return new ZoieContext();
    }
  };

  // use weakHashMap so that if a reader is no longer used, the map entry goes
  // away.
  protected WeakHashMap<ZoieIndexReader<?>, ZoieReaderContext> _zoieContextMap = new WeakHashMap<ZoieIndexReader<?>, ZoieReaderContext>();

  public static ZoieContext getContext() {
    return _context.get();
  }

  /**
   * Set the threadlocal ZoieContext. It always clone it so that the threads
   * won't interfere with each other.
   *
   * @param context
   */
  public static void setContext(ZoieContext context) {
    _context.set(context.clone());
  }

  /**
   * Get the context of the given reader for the calling thread.
   *
   * @param reader
   */
  public synchronized ZoieReaderContext getReaderContext(ZoieIndexReader<?> reader) {
    ZoieReaderContext ctx = _zoieContextMap.get(reader);
    if (ctx == null) {
      ctx = new ZoieReaderContext();
      _zoieContextMap.put(reader, ctx);
    }
    return ctx;
  }

  /**
   * clone the ZoieContext so that the internal hashmap is deep-copied to the
   * extend that in the clone, the ZoieReaderContexts are clones of the
   * original. Note that the internals of ZoieReaderContexts are shallow copies.
   *
   * @see java.lang.Object#clone()
   */
  @Override
  protected ZoieContext clone() {
    ZoieContext ctx = new ZoieContext();
    for (Entry<ZoieIndexReader<?>, ZoieReaderContext> pair : _zoieContextMap.entrySet()) {
      ctx._zoieContextMap.put(pair.getKey(), pair.getValue().clone());
    }
    return ctx;
  }

  public synchronized void clear() {
    _zoieContextMap.clear();
  }
}
