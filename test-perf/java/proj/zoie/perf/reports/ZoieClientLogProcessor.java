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

import org.deepak.performance.PerformanceManager;
import org.deepak.util.FilePlotData;
import org.deepak.util.GenericStatisticsUtil;
import org.deepak.util.PlotGraphs;
import org.jfree.chart.ChartUtilities;

public class ZoieClientLogProcessor
{

  private static String     timePatternString  =
                                                   "^(\\w+ \\d{1,2}, \\d{4} \\d{1,2}:\\d{1,2}:\\d{1,2} \\w{2}).* subTransaction\\d+";
  private static String     patternString      =
                                                   "^INFO: TIME-TAKEN: \\w+:(.*): (\\d+) : (\\d+)";

  private static Pattern    _pattern           = Pattern.compile(patternString);
  private static Pattern    _timePattern       = Pattern.compile(timePatternString);
  private static DateFormat _tdf               =
                                                   new SimpleDateFormat("MMM d, yyyy h:mm:ss aaa");
  private static DateFormat _df                =
                                                   new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  private static DateFormat _dfPlot            =
                                                   new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private Map               _map               = new HashMap();
  private Map               _avgMap            = new HashMap();
  private FilePlotData      _fpd               = null;
  private FilePlotData      _avgFpd            = null;
  public String             _fileName          = null;
  private static int        _imageHeight       = 300;
  private static int        _imageWidth        = 500;
  private static String[]   _additionalInfo    =
                                                   new String[] { "Total Transactions",
      "Execution Time", "Transaction Per Second", "Transaction Per Minute",
      "Total Executions", "Total Successful Executions", "Total Errors" };
  private long              totalTransasctions = -1;
  private long              totalExecutions    = -1;
  private long              totalErrors        = -1;
  private long              totalSuccess       = -1;
  private float             totalRunTime       = -1;
  private boolean           _isProcessed       = false;

  public ZoieClientLogProcessor(String fileName)
  {
    _fileName = fileName;
    process();
  }

  public void process()
  {
    System.out.println("Processing...");
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
      Date date = null;
      while ((line = br.readLine()) != null)
      {
        Matcher dateMatcher = _timePattern.matcher(line);
        if (dateMatcher.matches())
        {
          date = _dfPlot.parse(_dfPlot.format(_tdf.parse(dateMatcher.group(1))));
        }
        else
        {
          Matcher matcher = _pattern.matcher(line);
          if (matcher.matches())
          {
            cnt++;
            String queryStr = matcher.group(1).trim();
            long resultcnt = Long.parseLong(matcher.group(2).trim());
            long tmtaken = Long.parseLong(matcher.group(3).trim());
            if (date != null)
            {
              if (_avgMap.containsKey(date))
              {
                ZoieClientLogData tmp = (ZoieClientLogData) _avgMap.get(date);
                tmp.add(resultcnt, tmtaken);
              }
              else
              {
                _avgMap.put(date, new ZoieClientLogData(date,
                                                        queryStr,
                                                        resultcnt,
                                                        tmtaken));
              }
              ZoieClientLogData zd =
                  new ZoieClientLogData(date, queryStr, resultcnt, tmtaken);
              _map.put(new Integer(cnt), zd);
              date = null;
            }
          }
          else
          {
            if (line.startsWith("INFO: TOTAL "))
            {
              if (line.startsWith("INFO: TOTAL TRANSACTIONS:"))
              {
                totalTransasctions =
                    Long.parseLong(line.substring(line.lastIndexOf(" ")).trim());

              }
              else if (line.startsWith("INFO: TOTAL EXECUTIONS:"))
              {
                totalExecutions =
                    Long.parseLong(line.substring(line.lastIndexOf(" ")).trim());

              }
              else if (line.startsWith("INFO: TOTAL SUCCESSFUL EXECUTIONS:"))
              {
                totalSuccess =
                    Long.parseLong(line.substring(line.lastIndexOf(" ")).trim());

              }
              else if (line.startsWith("INFO: TOTAL FAILED EXECUTIONS:"))
              {
                totalErrors =
                    Long.parseLong(line.substring(line.lastIndexOf(" ")).trim());

              }
              else if (line.startsWith("INFO: TOTAL RUNTIME IN MINS:"))
              {
                totalRunTime =
                    Float.parseFloat(line.substring(line.lastIndexOf(" ")).trim());

              }
            }
          }
        }
      }

      _map = new TreeMap(_map);
      _avgMap = new TreeMap(_avgMap);

      br.close();

      Iterator itr = _map.keySet().iterator();
      StringBuffer plotStr = new StringBuffer("");
      while (itr.hasNext())
      {
        plotStr.append(((ZoieClientLogData) _map.get(itr.next())).toString() + "\n");
      }
      _fpd = new FilePlotData(plotStr.toString(), true);

      Iterator itr1 = _avgMap.keySet().iterator();

      StringBuffer plotStr1 = new StringBuffer("");
      if (itr1.hasNext())
      {
        while (itr1.hasNext())
        {
          plotStr1.append(((ZoieClientLogData) _avgMap.get(itr1.next())).averagedString()
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

  public static String getTableHtml(ZoieClientLogProcessor zp,
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

  public static String getTableHtml(ZoieClientLogProcessor zp) throws Exception
  {
    if (!zp._isProcessed)
    {
      zp.process();
    }
    if (zp._fpd == null)
    {
      return "";
    }
    String[] scenarios = new String[] { "Search Time (ms)", "Number Of Results" };
    String itemKey = "Item";
    GenericStatisticsUtil[] utils = new GenericStatisticsUtil[3];
    utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
    utils[0].process();
    utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
    utils[1].process();
    String tab =
        ZoieHtmlCreator.getSimpleTableHtmlString(itemKey, scenarios, utils, null);
    return tab;
  }

  public static String getTableHtmlForGraphs(ZoieClientLogProcessor zp,
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
          outputDir + File.separator + subFolder + "ZoieClientImage_" + (cnt - 1)
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

  public static String getTableHtmlForAdditionalData(ZoieClientLogProcessor zp) throws Exception
  {
    String tab = "";
    if (zp._fpd == null)
    {
      zp.process();
    }
    if (zp.totalTransasctions > 0)
    {
      if (zp.totalRunTime <= 0)
      {
        return "";
      }
      double qps =
          PerformanceManager.truncateNumber((zp.totalTransasctions)
              / (zp.totalRunTime * 60), 3);
      double qpm =
          PerformanceManager.truncateNumber((zp.totalTransasctions) / (zp.totalRunTime),
                                            3);
      String[] vals = new String[_additionalInfo.length];
      vals[0] = zp.totalTransasctions + "";
      vals[1] = zp.totalRunTime + "";
      vals[2] = qps + "";
      vals[3] = qpm + "";
      if (zp.totalExecutions > 0)
      {
        vals[4] = zp.totalTransasctions + "";
      }
      else
      {
        vals[4] = "Unknown";
      }

      if (zp.totalSuccess > 0)
      {
        vals[5] = zp.totalSuccess + "";
      }
      else
      {
        vals[5] = "Unknown";
      }
      if (zp.totalErrors >= 0)
      {
        vals[6] = zp.totalErrors + "";
      }
      else
      {
        vals[6] = "Unknown";
      }

      tab = ZoieHtmlCreator.getSimpleTableHtmlString(_additionalInfo, vals);

    }
    else
    {
      return "";
    }
    return tab;
  }

  public static class ZoieClientLogData
  {
    private Date   _date      = null;
    private String _query     = null;
    private long   _results   = -1;
    private long   _timeTaken = -1;
    private int    count      = 1;

    public ZoieClientLogData(Date date, String key, long results, long timetaken)
    {
      _date = date;
      _query = key;
      _results = results;
      _timeTaken = timetaken;
    }

    public void add(long results, long timetaken)
    {
      _results = _results + results;
      _timeTaken = _timeTaken + timetaken;
      count++;

    }

    public String averagedString()
    {
      String result =
          _dfPlot.format(_date) + "\t" + (_timeTaken / (double) count) + "\t"
              + (_results / (double) count);
      return result;
    }

    public String toString()
    {
      String result = _dfPlot.format(_date) + "\t" + _timeTaken + "\t" + _results;
      return result;
    }
  }

}
