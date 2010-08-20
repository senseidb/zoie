package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;

import proj.zoie.api.ZoieVersion;
import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.impl.indexing.internal.IndexSignature;
import proj.zoie.api.ZoieVersionFactory;
/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class HourglassDirectoryManagerFactory<V extends ZoieVersion>
{
  public static final Logger log = Logger.getLogger(HourglassDirectoryManagerFactory.class);

  private final File _root;
  private final HourGlassScheduler _scheduler;
  
  private volatile File _location;
  private volatile DirectoryManager<V> _currentDirMgr = null;
  private volatile boolean isRecentlyChanged = false;
  public final String dateFormatString = "yyyy-MM-dd-HH-mm-ss";
  private ThreadLocal<SimpleDateFormat> dateFormatter = new ThreadLocal<SimpleDateFormat>()
  {
    protected SimpleDateFormat initialValue()
    {
      return new SimpleDateFormat(dateFormatString);
    }
  }; 
  private volatile Calendar _nextUpdateTime = Calendar.getInstance();
  ZoieVersionFactory<V> _zoieVersionFactory;
  public HourglassDirectoryManagerFactory(File root, HourGlassScheduler scheduler, ZoieVersionFactory<V> zoieVersionFactory)
  {
    _root = root;
    _zoieVersionFactory = zoieVersionFactory;
    _scheduler = scheduler;
    log.info("starting HourglassDirectoryManagerFactory at " + root + " --- index rolling scheduler: " + _scheduler);
    updateDirectoryManager();
  }
  public DirectoryManager<V> getDirectoryManager()
  {
    return _currentDirMgr;
  }
  protected void setNextUpdateTime()
  {
    _nextUpdateTime = _scheduler.getNextRoll();
    log.info("setNextUpdateTime: " + _scheduler.getFolderName(_nextUpdateTime));
  }
  /**
   * @return true if the current index accepting updates is changed.
   * This method should be paired with clearRecentlyChanged() to clear the flag.
   * @see proj.zoie.hourglass.impl.HourglassDirectoryManagerFactory#clearRecentlyChanged()
   */
  public boolean updateDirectoryManager()
  {
    Calendar now = Calendar.getInstance();
    now.setTimeInMillis(System.currentTimeMillis());
    if (now.before(_nextUpdateTime)) return false;
    String folderName;
    folderName = _scheduler.getFolderName(_nextUpdateTime);
    _location = new File(_root, folderName);
    try
    {
      log.info("rolling forward with new path: " + _location.getCanonicalPath());
    } catch (IOException e)
    {
      log.error(e);
    }
    _currentDirMgr = new DefaultDirectoryManager<V>(_location, _zoieVersionFactory);
    isRecentlyChanged = true;
    setNextUpdateTime();
    return isRecentlyChanged;
  }
  public boolean isRecentlyChanged()
  {
    return isRecentlyChanged; 
  }
  public void clearRecentlyChanged()
  {
    isRecentlyChanged = false;
  }

  public File getRoot()
  {
    return _root;
  }
  public long getDiskIndexSizeBytes()
  {
    return FileUtil.sizeFile(_root);
  }

  /**
   * @return a list that contains all the archived index directories excluding the one
   * currently accepting updates.
   */
  public List<Directory> getAllArchivedDirectories()
  {
    @SuppressWarnings("unchecked")
    List<Directory> emptyList = Collections.EMPTY_LIST;
    if (!_root.exists()) return emptyList;
    File[] files = _root.listFiles();
    Arrays.sort(files);
    ArrayList<Directory> list = new ArrayList<Directory>();
    for(File file : files)
    {
      String name = file.getName();
      log.debug("getAllArchivedDirectories" + name + " " + (file.equals(_location)?"*":""));
      long time = 0;
      try
      {
        time = dateFormatter.get().parse(name).getTime();
      } catch (ParseException e)
      {
        log.warn("potential index corruption. we skip folder: " + name, e);
        continue;
      }
      if (!file.equals(_location))
      { // don't add the current one
        try
        {
          list.add(new SimpleFSDirectory(file));
        } catch (IOException e)
        {
          log.error("potential index corruption", e);
        }
      }
    }
    if (list.size()==0) return emptyList;
    return list;
  }
  /**
   * @return the max version from all the archived index
   */
  public V getArchivedVersion()
  {
    if (!_root.exists()) return null;
    V version = null;
    File[] files = _root.listFiles();
    Arrays.sort(files);
    for(File file : files)
    {
      String name = file.getName();
      log.debug("getAllArchivedDirectories" + name + " " + (file.equals(_location)?"*":""));
      long time = 0;
      try
      {
        time = dateFormatter.get().parse(name).getTime();
      } catch (ParseException e)
      {
        log.warn("potential index corruption. we skip folder: " + name, e);
        continue;
      }
      if (!file.equals(_location))
      { // don't count the current one
        IndexSignature<V> sig = getIndexSignature(file);
        if (sig!=null)
        {
          //if (sig.getVersion() > version) version = sig.getVersion();
          if(sig.getVersion() != null)
          {
            if(sig.getVersion().compareTo(version) >0)
            {
              version = sig.getVersion();
            }
          }
        } else
        {
          log.error("potential index corruption: indexSignature not in " + _location);
        }
      }
    }
    return version;
  }
  public IndexSignature<V> getIndexSignature(File file)
  {
    File directoryFile = new File(file, DirectoryManager.INDEX_DIRECTORY);
    IndexSignature<V> sig = DefaultDirectoryManager.readSignature(directoryFile, _zoieVersionFactory);
    return sig;
  }
  
  public void saveIndexSignature(File tgt, IndexSignature<V> sig) throws IOException
  {
    File tgtFile = new File(tgt, DirectoryManager.INDEX_DIRECTORY);
    DefaultDirectoryManager.saveSignature(sig, tgtFile);
  }
  public Calendar getCalendarTime(String date) throws ParseException
  {
    long time;
    try
    {
      time = dateFormatter.get().parse(date).getTime();
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(time);
      return cal;
    } catch (ParseException e)
    {
      log.error("date formate should be like " + dateFormatString, e);
      throw e;
    }
  }
}
