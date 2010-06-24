package proj.zoie.api;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.ZoieVersionFactory;

import org.apache.lucene.store.Directory;

public interface DirectoryManager<V extends ZoieVersion>
{
  static final String INDEX_DIRECTORY = "index.directory";

  Directory getDirectory() throws IOException;
  
  Directory getDirectory(boolean create) throws IOException;
  
  V getVersion() throws IOException;  
  void setVersion(V version) throws IOException;
  
  //ZoieVersionFactory<V> getVersionFactory();
  //void setVersionFactory(ZoieVersionFactory<V> zoieVersionFactory);

  Date getLastIndexModifiedTime();

  String getPath();
  
  void purge();
  
  boolean exists();
  
  boolean transferFromChannelToFile(ReadableByteChannel channel, String fileName) throws IOException;

  long transferFromFileToChannel(String fileName, WritableByteChannel channel) throws IOException;
}
