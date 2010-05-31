package proj.zoie.hourglass.impl;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
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
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class HourglassDirectoryManagerFactory
{
  public static final Logger log = Logger.getLogger(HourglassDirectoryManagerFactory.class);

  private long _indexDuration = 10000L;

  private final File _root;
  
  private volatile File _location;
  private volatile DirectoryManager _currentDirMgr = null;
  private volatile boolean isRecentlyChanged = false;
//  private DecimalFormat formatter = new DecimalFormat("00000000000000000000");
  private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd-HHmmssSSS"); 
  private volatile long _nextUpdateTime=0L;
  public HourglassDirectoryManagerFactory(File root, long duration)
  {
    _root = root;
    if (duration > _indexDuration)
    {
      _indexDuration = duration;
    }
    log.info("starting HourglassDirectoryManagerFactory at " + root + " --- index rolling duration: " + duration +"ms");
    updateDirectoryManager();
  }
  public DirectoryManager getDirectoryManager()
  {
    return _currentDirMgr;
  }
  protected void setNextUpdateTime()
  {
    Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    long currentTimeMillis = System.currentTimeMillis();
    now.setTimeInMillis(currentTimeMillis);
    long currentPeriod = getPeriod(now.getTimeInMillis());
    _nextUpdateTime = currentTimeMillis + currentPeriod + _indexDuration - now.getTimeInMillis();
  }
  public boolean updateDirectoryManager()
  {
    if (System.currentTimeMillis()< _nextUpdateTime) return false;
    Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    now.setTimeInMillis(System.currentTimeMillis());
    _location = new File(_root, getFolderName(now)+"/"+DirectoryManager.INDEX_DIRECTORY);
    try
    {
      System.out.println(_location.getCanonicalPath());
    } catch (IOException e)
    {
      log.error(e);
    }
    _currentDirMgr = new DefaultDirectoryManager(_location);
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

  private long getPeriod(long time)
  {
    return time - (time % _indexDuration);
  }
  
  public boolean exists(Calendar cal)
  {
    File folder = new File(_root, getFolderName(cal)+"/"+DirectoryManager.INDEX_DIRECTORY);
    return folder.exists();
  }
  
  private String getFolderName(Calendar cal)
  {
    Calendar mycal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
    mycal.setTimeInMillis(getPeriod(cal.getTimeInMillis()));
    return dateFormatter.format(mycal.getTime());
  }

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
      System.out.println(name);
      long time = 0;
      try
      {
        time = dateFormatter.parse(name).getTime();
      } catch (ParseException e)
      {
        log.warn("potential index corruption", e);
        System.out.println("potential index corruption"+ e);
        continue;
      }
      if (time != getPeriod(time))
      {
        log.warn("potential index corruption. Folder name "+ dateFormatter.format(time) + " inconsistent with period: " + _indexDuration );
        System.out.println("potential index corruption. Folder name "+ dateFormatter.format(time) + " inconsistent with period: " + _indexDuration );
        continue;
      }
      if (!file.equals(_location))
      {
        try
        {
          list.add(FSDirectory.open(file));
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
   * @param param
   * @return
   */
  public List<Directory> getArchivedDirectories(
      List<Calendar> param)
  {
    return null;
  }
}
