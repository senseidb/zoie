package proj.zoie.api;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;

import org.apache.lucene.store.Directory;

public interface DirectoryManager {
  static final String INDEX_DIRECTORY = "index.directory";

  Directory getDirectory() throws IOException;

  Directory getDirectory(boolean create) throws IOException;

  String getVersion() throws IOException;

  void setVersion(String version) throws IOException;

  // ZoieVersionFactory<V> getVersionFactory();
  // void setVersionFactory(ZoieVersionFactory<V> zoieVersionFactory);

  Date getLastIndexModifiedTime();

  String getPath();

  void purge();

  boolean exists();

  boolean transferFromChannelToFile(ReadableByteChannel channel, String fileName)
      throws IOException;

  long transferFromFileToChannel(String fileName, WritableByteChannel channel) throws IOException;

  public static enum DIRECTORY_MODE {
    /**
     * Provides directories in using SimpleFSDirectory, which has the best compatibility.
     * It is the default mode used by Zoie.
     */
    SIMPLE,
    /**
     * Provides directories in using NIOFSDirectory. <b>WARNING</b>: In this mode,
     * if searching threads are interrupted during search, the underlying channel
     * may be closed and the ongoing and subsequent searches will all fail.
     * Zoie will not be notified of this closure and cannot recover from such event
     * until the involved index segment is obsolete and removed from reader list.
     * It is advised that this not used when search threads are managed by
     * Thread.interrupt and/or Future.cancel, or other forms of thread life
     * cycle management that implicitly uses Thread.interrupe.
     */
    NIO,
    /**
     * Provides directories in using MMapDirectory, which is faster than SimpleFSDirectory. It requires
     * sufficient virtual memory address space to map the entire file. If system does not have enough
     * memory, it will cause performance loss due to swaps.
     */
    MMAP
  }
}
