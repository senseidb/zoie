package proj.zoie.perf.reports;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.deepak.util.FilePlotData;
import org.deepak.util.GenericStatisticsUtil;
import org.deepak.util.PlotGraphs;
import org.jfree.chart.ChartUtilities;

public class ZoieServerLogProcessor
{
  private static String     patternString =
                                              "^(\\d{4}\\/\\d{2}\\/\\d{2} \\d{1,2}:\\d{1,2}:\\d{1,2}.\\d{3}) INFO \\[MonitoredZoieService\\] .*\\[numSearchResults=(\\d+)\\].*in (\\d+)ms";

  private static Pattern    _pattern      = Pattern.compile(patternString);
  private static DateFormat _tdf          =
                                              new SimpleDateFormat("MMM d, yyyy h:mm:ss aaa");
  private static DateFormat _df           =
                                              new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  private static DateFormat _dfPlot       = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private Map               _map          = new HashMap();
  private Map               _avgMap       = new HashMap();
  private FilePlotData      _fpd          = null;
  private FilePlotData      _avgFpd       = null;
  public String             _fileName     = null;
  private static int        _imageHeight  = 300;
  private static int        _imageWidth   = 500;
  private boolean           _isProcessed  = false;

  public ZoieServerLogProcessor(String fileName)
  {
    _fileName = fileName;
    process();
  }

  public void process()
  {
    if (_isProcessed)
    {
      return;
    }
    try
    {
      _isProcessed = true;
      BufferedReader br = new BufferedReader(new FileReader(new File(_fileName)));
      String line = null;
      int cnt = 0;
      while ((line = br.readLine()) != null)
      {
        Matcher matcher = _pattern.matcher(line);
        if (matcher.matches())
        {
          cnt++;
          String dtStr = matcher.group(1);
          Date date = _df.parse(dtStr);
          Date date1 = _dfPlot.parse(dtStr);
          long resultcnt = Long.parseLong(matcher.group(2).trim());
          long tmtkn = Long.parseLong(matcher.group(3).trim());
          if (_avgMap.containsKey(date1))
          {
            ZoieServerData tmp = (ZoieServerData) _avgMap.get(date1);
            tmp.add(resultcnt, tmtkn);
          }
          else
          {
            _avgMap.put(date1, new ZoieServerData(date, resultcnt, tmtkn));
          }

          ZoieServerData zid = new ZoieServerData(date, resultcnt, tmtkn);
          _map.put(new Integer(cnt), zid);

        }
      }

      _map = new TreeMap(_map);
      _avgMap = new TreeMap(_avgMap);

      br.close();

      Iterator itr = _map.keySet().iterator();

      StringBuffer plotStr = new StringBuffer("");
      if (itr.hasNext())
      {
        while (itr.hasNext())
        {
          plotStr.append(((ZoieServerData) _map.get(itr.next())).toString() + "\n");
        }
        _fpd = new FilePlotData(plotStr.toString(), true);

      }

      Iterator itr1 = _avgMap.keySet().iterator();

      StringBuffer plotStr1 = new StringBuffer("");
      if (itr1.hasNext())
      {
        while (itr1.hasNext())
        {
          plotStr1.append(((ZoieServerData) _avgMap.get(itr1.next())).averagedString()
              + "\n");
        }
        _avgFpd = new FilePlotData(plotStr1.toString(), true);

      }

    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public static String getTableHtmlForGraphs(ZoieServerLogProcessor zp,
                                             String outputDir,
                                             String subFolderForOutput,
                                             String seriesDescription,
                                             int imgWidth,
                                             int imgHeight,
                                             boolean useTimeStamp,
                                             boolean useAbsFilePath,
                                             String alignment,
                                             int numGraphsPerRow) throws Exception
  {
    int number = 2;
    if (numGraphsPerRow > 0)
    {
      number = numGraphsPerRow;
    }
    if (!zp._isProcessed)
    {
      zp.process();
    }
    if (zp._avgFpd == null)
    {
      return "";
    }
    String subFolder = "";
    if ((subFolderForOutput != null) && (!"".equals(subFolderForOutput.trim())))
    {
      subFolder = subFolder + File.separator;
    }
    int imageWidth = imgWidth;
    int imageHeight = imgHeight;

    if ((imageWidth <= 0) || (imageHeight <= 0))
    {
      imageHeight = _imageHeight;
      imageWidth = _imageWidth;
    }

    Map<Integer, String> inMap = new HashMap<Integer, String>();
    inMap.put(new Integer(2), "Search Time (ms)");
    inMap.put(new Integer(3), "Number Of Results");

    inMap = new TreeMap<Integer, String>(inMap);
    Iterator<Integer> itr = inMap.keySet().iterator();
    int cnt = 2;
    List list = new ArrayList();

    while (itr.hasNext())
    {
      Integer key = itr.next();
      String yTitle = (String) inMap.get(key);
      int indexVal = key.intValue();
      String chartTitle = yTitle + " Vs. Time";
      BufferedImage image =
          PlotGraphs.createXYTimeImage(zp._avgFpd,
                                       1,
                                       new int[] { indexVal },
                                       chartTitle,
                                       "Time",
                                       yTitle,
                                       new String[] { seriesDescription },
                                       _dfPlot,
                                       imageWidth,
                                       imageHeight);
      String tmpAdd = "";
      if (useTimeStamp)
      {
        tmpAdd = "_" + ZoieHtmlCreator.getTimeStamp();
      }

      String fileName =
          outputDir + File.separator + subFolder + "ZoieServerImage_" + (cnt - 1)
              + tmpAdd + ".png";
      File pngFile = new File(fileName);
      if (useAbsFilePath)
      {
        list.add(pngFile.getAbsolutePath());
      }
      else
      {
        list.add(subFolder + pngFile.getName());
      }
      OutputStream ost = new BufferedOutputStream(new FileOutputStream(pngFile, false));
      ChartUtilities.writeBufferedImageAsPNG(ost, image);
      System.out.println("Image " + (cnt - 1) + " : " + fileName);
      cnt++;
    }

    String[] graphs = new String[list.size()];
    Iterator itr1 = list.iterator();
    for (int i = 0; i < graphs.length; i++)
    {
      graphs[i] = (String) itr1.next();
    }
    String tab =
        ZoieHtmlCreator.getSimpleTableHtmlStringForGraphs(graphs, alignment, number);
    return tab;
  }

  public static String getTableHtml(ZoieServerLogProcessor zp) throws Exception
  {
    if (zp._fpd == null)
    {
      zp.process();
    }
    String[] scenarios =
        new String[] { "Search Time (ms)", "Number Of Results Returned" };
    String itemKey = "Item";
    GenericStatisticsUtil[] utils = new GenericStatisticsUtil[2];
    utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
    utils[0].process();
    utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
    utils[1].process();

    String tab =
        ZoieHtmlCreator.getSimpleTableHtmlString(itemKey, scenarios, utils, null);
    return tab;
  }

  public static String getTableHtml(ZoieServerLogProcessor zp,
                                    boolean addGraphs,
                                    String outputDir,
                                    String subFolderForOutput,
                                    int imgWidth,
                                    int imgHeight,
                                    int numGraphsPerRow) throws Exception
  {
    if (zp._fpd == null)
    {
      zp.process();
    }
    String[] scenarios =
        new String[] { "Search Time (ms)", "Number Of Results Returned" };
    String itemKey = "Item";
    GenericStatisticsUtil[] utils = new GenericStatisticsUtil[2];
    utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
    utils[0].process();
    utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
    utils[1].process();

    String tab =
        ZoieHtmlCreator.getSimpleTableHtmlString(itemKey, scenarios, utils, null);
    if (addGraphs)
    {
      tab =
          tab
              + "<br><br>\n"
              + getTableHtmlForGraphs(zp,
                                      outputDir,
                                      subFolderForOutput,
                                      "Series",
                                      imgWidth,
                                      imgHeight,
                                      true,
                                      false,
                                      "center",
                                      numGraphsPerRow);
    }
    return tab;
  }

  public static class ZoieServerData
  {
    private long _timeTaken       = -1;
    private long _resultsReturned = -1;
    private Date _date            = null;
    private int  count            = 1;

    public ZoieServerData(Date date, long rescnt, long time)
    {
      _date = date;
      _resultsReturned = rescnt;
      _timeTaken = time;
    }

    public void add(long rescnt, long time)
    {
      _resultsReturned = _resultsReturned + rescnt;
      _timeTaken = _timeTaken + time;
      count++;
    }

    public Date getDate()
    {
      return _date;
    }

    public String averagedString()
    {
      String result =
          _dfPlot.format(_date) + "\t" + (_timeTaken / (double) count) + "\t"
              + (_resultsReturned / (double) count);
      return result;
    }

    public String toString()
    {
      String result = _df.format(_date) + "\t" + _timeTaken + "\t" + _resultsReturned;
      return result;
    }
  }

}
