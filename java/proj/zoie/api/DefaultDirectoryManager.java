package proj.zoie.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.impl.util.ChannelUtil;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.impl.indexing.internal.IndexSignature;

public class DefaultDirectoryManager implements DirectoryManager
{
  public static final Logger log = Logger.getLogger(DefaultDirectoryManager.class);

  private File _location;
  
  public DefaultDirectoryManager(File location)
  {
    if (location==null) throw new IllegalArgumentException("null index directory.");

    _location = location;
  }
  
  public File getLocation()
  {
    return _location;
  }
  
  public List<Directory> getAllArchivedDirectories()
  {
    throw new UnsupportedOperationException();
  }

  public Directory getDirectory() throws IOException
  {
    return getDirectory(false);
  }
  
  public Directory getDirectory(boolean create) throws IOException
  {
    if(!_location.exists() && create)
    {
      // create the parent directory
      _location.mkdirs();
    }
    
    if(create)
    {
      IndexSignature sig = null;
      if (_location.exists())
      {
        sig = getCurrentIndexSignature();
      }
      
      if (sig == null)
      {
        File directoryFile = new File(_location, INDEX_DIRECTORY);
        sig = new IndexSignature(0L);
        try
        {
          saveSignature(sig, directoryFile);
        }
        catch (IOException e)
        {
          throw e;
        }
      }
    }
    
    return FSDirectory.open(_location);
  }
  
  public static IndexSignature readSignature(File file)
  {
    if (file.exists())
    {
      FileInputStream fin = null;
      try
      {
        fin = new FileInputStream(file);
        return IndexSignature.read(fin);
      }
      catch (IOException ioe)
      {
        log.error("Problem reading index directory file.", ioe);
        return null;
      }
      finally
      {
        if (fin != null)
        {
          try
          {
            fin.close();
          }
          catch (IOException e)
          {
            log.warn("Problem closing index directory file: " + e.getMessage());
          }
        }
      }
    }
    else
    {
      log.info("Starting with empty search index: version information not found");
      return null;
    }
  }
  
  private void saveSignature(IndexSignature sig, File file) throws IOException
  {
    if (!file.exists())
    {
      file.createNewFile();
    }
    FileOutputStream fout = null;
    try
    {
      fout = new FileOutputStream(file);
      sig.save(fout);
    }
    finally
    {
      if (fout != null)
      {
        try
        {
          fout.close();
        }
        catch (IOException e)
        {
          log.warn("Problem closing index directory file: " + e.getMessage());
        }
      }
    }
  }
   
  /**
   * Gets the current signature
   * @param indexHome
   * @return
   */
  public IndexSignature getCurrentIndexSignature()
  {
    File directoryFile = new File(_location, INDEX_DIRECTORY);
    IndexSignature sig = readSignature(directoryFile);
    return sig;
  }

  public long getVersion() throws IOException
  {
    IndexSignature sig = getCurrentIndexSignature();
    return sig == null ? 0L : sig.getVersion();
  }
  
  public void setVersion(long version) throws IOException
  {
    // update new index file
    File directoryFile = new File(_location, INDEX_DIRECTORY);
    IndexSignature sig = readSignature(directoryFile);
    sig.updateVersion(version);
    try
    {
      // make sure atomicity of the index publication
      File tmpFile = new File(_location, INDEX_DIRECTORY + ".new");
      saveSignature(sig, tmpFile);
      File tmpFile2 = new File(_location, INDEX_DIRECTORY + ".tmp");
      directoryFile.renameTo(tmpFile2);
      tmpFile.renameTo(directoryFile);
      tmpFile2.delete();
    }
    catch (IOException e)
    {
      throw e;
    }
  }
  
  public Date getLastIndexModifiedTime()
  {
    File directoryFile = new File(_location, INDEX_DIRECTORY);
    return new Date(directoryFile.lastModified());      
  }
  
  public String getPath()
  {
    return _location.getAbsolutePath();
  }
  
  public void purge()
  { 
    FileUtil.rmDir(_location);
  }
  
  public boolean exists()
  {
    return _location.exists();
  }
  
  public boolean transferFromChannelToFile(ReadableByteChannel channel, String fileName) throws IOException
  {
    if(!_location.exists())
    {
      // create the parent directory
      _location.mkdirs();
    }
 
    long dataLen = ChannelUtil.readLong(channel);
    if(dataLen < 0) return false;
    
    File file = new File(_location, fileName);
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try
    {
      raf = new RandomAccessFile(file, "rw");
      fc = raf.getChannel();
      return (fc.transferFrom(channel, 0, dataLen) == dataLen);
    }
    finally
    {
      if(raf != null) raf.close();
    }
  }
  
  public long transferFromFileToChannel(String fileName, WritableByteChannel channel) throws IOException
  {
    long amount = 0;
    File file = new File(_location, fileName);
    RandomAccessFile raf = null;
    FileChannel fc = null;
    try
    {
      raf = new RandomAccessFile(file, "rw");
      fc = raf.getChannel();
      long dataLen = fc.size();
      amount += ChannelUtil.writeLong(channel, dataLen);
      amount += fc.transferTo(0, dataLen, channel);
    }
    finally
    {
      if(raf != null) raf.close();
    }
    return amount;
  }
}
