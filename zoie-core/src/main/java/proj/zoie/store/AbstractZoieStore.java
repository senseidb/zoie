package proj.zoie.store;

import it.unimi.dsi.fastutil.longs.Long2ObjectLinkedOpenHashMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.apache.lucene.util.BytesRef;

import proj.zoie.impl.indexing.ZoieConfig;

public abstract class AbstractZoieStore implements ZoieStore {

  private static Logger logger = Logger.getLogger(AbstractZoieStore.class);
  private boolean _dataCompressed = true;
  private Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;

  private volatile String _version = null;
  private final Long2ObjectLinkedOpenHashMap<byte[]> _dataMap = new Long2ObjectLinkedOpenHashMap<byte[]>();

  private final ReentrantReadWriteLock _dataMapLock = new ReentrantReadWriteLock();
  private final WriteLock _writeLock = _dataMapLock.writeLock();
  private final ReadLock _readLock = _dataMapLock.readLock();

  public void setVersionComparator(Comparator<String> versionComparator) {
    _versionComparator = versionComparator;
  }

  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }

  public void setDataCompressed(boolean dataCompressed) {
    _dataCompressed = dataCompressed;
  }

  public boolean isDataCompressed() {
    return _dataCompressed;
  }

  protected abstract void persist(long uid, byte[] data) throws IOException;

  protected abstract void persistDelete(long uid) throws IOException;

  protected abstract BytesRef getFromStore(long uid) throws IOException;

  protected abstract void commitVersion(String version) throws IOException;

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract void open() throws IOException;

  public static byte[] compress(byte[] src) throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    GZIPOutputStream gzipStream = new GZIPOutputStream(bout);

    gzipStream.write(src);
    gzipStream.flush();
    gzipStream.close();
    bout.flush();

    return bout.toByteArray();
  }

  public static byte[] uncompress(byte[] src) throws IOException {
    byte[] buffer = new byte[1024]; // 1k buffer
    ByteArrayInputStream bin = new ByteArrayInputStream(src);
    GZIPInputStream gzipIn = new GZIPInputStream(bin);
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    while (gzipIn.available() > 0) {
      int len = gzipIn.read(buffer);
      if (len <= 0) break;
      if (len < buffer.length) {
        bout.write(buffer, 0, len);
      } else {
        bout.write(buffer);
      }
    }
    bout.flush();
    return bout.toByteArray();
  }

  @Override
  public void commit() throws IOException {
    _writeLock.lock();
    try {
      commitVersion(_version);
      _dataMap.clear();
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public void delete(long uid, String version) throws IOException {
    _writeLock.lock();
    try {
      persistDelete(uid);
      _dataMap.remove(uid);
      _version = version;
    } finally {
      _writeLock.unlock();
    }
  }

  @Override
  public final void put(long uid, byte[] data, String version) throws IOException {
    if (_dataCompressed) {
      data = compress(data);
    }
    _writeLock.lock();
    try {
      persist(uid, data);
      _dataMap.put(uid, data);
      _version = version;
    } finally {
      _writeLock.unlock();

    }
  }

  private final byte[] innerGet(long uid) throws IOException {
    byte[] data = null;
    try {
      _readLock.lock();
      data = _dataMap.get(uid);
    } finally {
      _readLock.unlock();
    }
    if (data == null) {
      data = getFromStore(uid).bytes;
    }
    if (data != null && _dataCompressed) {
      data = uncompress(data);
    }
    return data;
  }

  @Override
  public final byte[] get(long uid) throws IOException {
    _readLock.lock();
    try {
      return innerGet(uid);
    } finally {
      _readLock.unlock();
    }
  }

  @Override
  public final String getVersion() {
    return _version;
  }

  @Override
  public byte[][] get(long[] uids) {
    byte[][] dataList = new byte[uids.length][];
    int idx = 0;
    for (long uid : uids) {
      try {
        dataList[idx++] = innerGet(uid);
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }
    }

    return dataList;
  }
}
