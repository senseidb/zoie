package proj.zoie.store;

public interface ZoieStoreSerializer<D> {
  long getUid(D data);

  byte[] toBytes(D data);
}
