package proj.zoie.store;

public interface ZoieStoreSerializer<D> {
  long getUid(D data);

  byte[] toBytes(D data);

  D fromBytes(byte[] data);

  boolean isDelete(D data);

  boolean isSkip(D data);
}
