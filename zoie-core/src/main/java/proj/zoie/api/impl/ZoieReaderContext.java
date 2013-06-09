package proj.zoie.api.impl;

import java.util.HashMap;

import proj.zoie.api.ZoieMultiReader;

public class ZoieReaderContext {
  protected HashMap<String, Object> _contextMap = new HashMap<String, Object>();

  public void set(String key, Object value) {
    _contextMap.put(key, value);
  }

  public Object get(String key) {
    return _contextMap.get(key);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key, Class<T> clazz) {
    return (T) _contextMap.get(key);
  }

  @Override
  protected ZoieReaderContext clone() {
    ZoieReaderContext ctx = new ZoieReaderContext();
    ctx._contextMap.putAll(_contextMap);
    return ctx;
  }

  public static class ContextAccessor<E> {
    private final String _key;
    private final ZoieMultiReader<?> _reader;

    public ContextAccessor(ZoieMultiReader<?> reader, String key) {
      _key = key.intern();
      _reader = reader;
    }

    public void set(E value) {
      ZoieContext.getContext().getReaderContext(_reader).set(_key, value);
    }

    @SuppressWarnings("unchecked")
    public E get() {
      return (E) ZoieContext.getContext().getReaderContext(_reader).get(_key);
    }
  }
}
