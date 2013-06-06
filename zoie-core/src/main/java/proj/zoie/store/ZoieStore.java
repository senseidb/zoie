package proj.zoie.store;

import java.io.IOException;

public interface ZoieStore {
  void put(long uid, byte[] data, String version) throws IOException;

  byte[] get(long uid) throws IOException;

  byte[][] get(long[] uids);

  String getVersion();

  void delete(long uid, String version) throws IOException;

  void open() throws IOException;

  void close() throws IOException;

  void commit() throws IOException;
}
