package proj.zoie.perf.reports;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.*;
import java.text.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import java.util.List;

import org.apache.commons.cli.*;
import org.deepak.performance.PerformanceManager;
import org.deepak.util.FilePlotData;
import org.deepak.util.GenericStatisticsUtil;
import org.deepak.util.PdfDoc;
import org.deepak.util.PlotGraphs;
import org.jfree.chart.*;

import com.lowagie.text.*;

public class ZoieIndexLogProcessor
{
  private static String     patternString =
                                              "^(\\d{4}\\/\\d{2}\\/\\d{2} \\d{1,2}:\\d{1,2}:\\d{1,2}.\\d{3}) INFO \\[BatchedIndexDataLoader\\] .*batch of (\\d+).*: (\\d+).*: (\\d+).*";
  private static Pattern    _pattern      = Pattern.compile(patternString);
  private static DateFormat _df           =
                                              new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  private static DateFormat _dfPlot       = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private Map               _map          = new HashMap();
  private FilePlotData      _fpd          = null;
  public String             _fileName     = null;
  private static int        _imageHeight  = 300;
  private static int        _imageWidth   = 500;
  private boolean           _isProcessed  = false;

  public ZoieIndexLogProcessor(String fileName)
  {
    _fileName = fileName;
    if (_fileName != null)
    {
      process();
    }
  }

  private void process()
  {

    if (_isProcessed || (_fileName == null))
    {
      return;
    }
    _isProcessed = true;
    try
    {
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
          Date date = _dfPlot.parse(dtStr);
          long tmsnc = _df.parse(dtStr).getTime();
          long eventFlush = Long.parseLong(matcher.group(2).trim());
          long tmtkn = Long.parseLong(matcher.group(3).trim());
          long count = Long.parseLong(matcher.group(4).trim());

          ZoieIndexLogData zid =
              new ZoieIndexLogData(date, tmsnc, eventFlush, count, tmtkn);
          _map.put(new Integer(cnt), zid);

        }
      }

      _map = new TreeMap(_map);

      br.close();

      Iterator itr = _map.keySet().iterator();
      ZoieIndexLogData first = null;
      String plotStr = "";
      if (itr.hasNext())
      {
        first = (ZoieIndexLogData) _map.get(itr.next());
        while (itr.hasNext())
        {
          ZoieIndexLogData second = (ZoieIndexLogData) _map.get(itr.next());
          plotStr = plotStr + first.diff(second) + "\n";
          first = second;
        }
        _fpd = new FilePlotData(plotStr, true);

      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }

  public static String getTableHtml(ZoieIndexLogProcessor zp) throws Exception
  {
    if (!zp._isProcessed)
    {
      zp.process();
    }
    if (zp._fpd == null)
    {
      return "";
    }
    String[] scenarios =
        new String[] { "Time to fill RAM Buffer (ms)", "Average Flush Rate (per second)",
            "Items Flushed" };
    String itemKey = "Item";
    GenericStatisticsUtil[] utils = new GenericStatisticsUtil[3];
    utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
    utils[0].process();
    utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
    utils[1].process();
    utils[2] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(4));
    utils[2].process();
    String tab =
        ZoieHtmlCreator.getSimpleTableHtmlString(itemKey, scenarios, utils, null);
    return tab;
  }

  public static String getTableHtml(ZoieIndexLogProcessor zp,
                                    boolean addGraphs,
                                    String outputDir,
                                    String subFolderForOutput,
                                    int imgWidth,
                                    int imgHeight,
                                    int numGraphsPerRow) throws Exception
  {
    if (!zp._isProcessed)
    {
      zp.process();
    }
    if (zp._fpd == null)
    {
      return "";
    }
    String[] scenarios =
        new String[] { "Time to fill RAM Buffer (ms)", "Average Flush Rate (per second)",
            "Items Flushed" };
    String itemKey = "Item";
    GenericStatisticsUtil[] utils = new GenericStatisticsUtil[3];
    utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
    utils[0].process();
    utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
    utils[1].process();
    utils[2] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(4));
    utils[2].process();
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

  public static String getTableHtmlForGraphs(ZoieIndexLogProcessor zp,
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
    if (zp._fpd == null)
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
    inMap.put(new Integer(2), "Time to fill RAM Buffer (ms)");
    inMap.put(new Integer(3), "Average Flush Rate (per second)");
    inMap.put(new Integer(4), "Items Flushed");
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
          PlotGraphs.createXYTimeImage(zp._fpd,
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
          outputDir + File.separator + subFolder + "ZoieIndexImage_" + (cnt - 1) + tmpAdd
              + ".png";
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

  public static void plot(ZoieIndexLogProcessor zp,
                          String outputFile,
                          String seriesDescription,
                          int imgWidth,
                          int imgHeight,
                          boolean createPngs,
                          String pngDir,
                          boolean createHtml,
                          String htmlOutFile) throws Exception
  {
    if (zp._fpd == null)
    {
      zp.process();
    }

    if (zp._fpd == null)
    {
      return;
    }

    int imageWidth = imgWidth;
    int imageHeight = imgHeight;

    if ((imageWidth <= 0) || (imageHeight <= 0))
    {
      imageHeight = _imageHeight;
      imageWidth = _imageWidth;
    }

    String pngDirPath = null;
    if (createPngs == true)
    {
      if (pngDir == null)
      {
        pngDirPath = new File("").getAbsolutePath();
      }
      else
      {
        pngDirPath = new File(pngDir).getAbsolutePath();
      }
      File pngDirFile = new File(pngDirPath);
      if (pngDirFile != null)
      {
        pngDirFile.mkdirs();
      }
    }
    System.setProperty("java.awt.headless", "true");
    String plotFile = outputFile;
    PdfDoc doc = new PdfDoc(plotFile);
    Font font = FontFactory.getFont(FontFactory.COURIER, 16, Font.ITALIC, Color.BLUE);

    Map<Integer, String> inMap = new HashMap<Integer, String>();
    inMap.put(new Integer(2), "Time to fill RAM Buffer (ms)");
    inMap.put(new Integer(3), "Average Flush Rate (per second)");
    inMap.put(new Integer(4), "Items Flushed");
    inMap = new TreeMap<Integer, String>(inMap);
    Iterator<Integer> itr = inMap.keySet().iterator();
    int cnt = 2;

    while (itr.hasNext())
    {
      Integer key = itr.next();
      String yTitle = (String) inMap.get(key);
      int indexVal = key.intValue();
      String chartTitle = yTitle + " Vs. Time";
      Phrase phrase = new Phrase(chartTitle.toUpperCase(), font);
      Chapter chapter = new Chapter(new Paragraph(phrase), (cnt - 1));
      BufferedImage image =
          PlotGraphs.createXYTimeImage(zp._fpd,
                                       1,
                                       new int[] { indexVal },
                                       chartTitle,
                                       "Time",
                                       yTitle,
                                       new String[] { seriesDescription },
                                       _dfPlot,
                                       imageWidth,
                                       imageHeight);
      if (createPngs)
      {
        String fileName =
            pngDirPath + File.separator + "ZoieIndexImage_" + (cnt - 1) + ".png";
        OutputStream ost =
            new BufferedOutputStream(new FileOutputStream(new File(fileName), false));
        ChartUtilities.writeBufferedImageAsPNG(ost, image);
        System.out.println("Image " + (cnt - 1) + " : " + fileName);
      }

      doc.addSingleElement(chapter);
      doc.addMultipleNewLine(4);
      doc.addSingleElement(image);
      doc.addNewPage();
      cnt++;

    }
    doc.closeDocument();

    if (createHtml)
    {
      String[] scenarios =
          new String[] { "Time to fill RAM Buffer (ms)",
              "Average Flush Rate (per second)", "Items Flushed" };
      String itemKey = "Item";
      GenericStatisticsUtil[] utils = new GenericStatisticsUtil[3];
      utils[0] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(2));
      utils[0].process();
      utils[1] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(3));
      utils[1].process();
      utils[2] = new GenericStatisticsUtil(zp._fpd.getNonNumericNthColumn(4));
      utils[2].process();

      try
      {
        String tab =
            ZoieHtmlCreator.getSimpleTableHtmlString(itemKey, scenarios, utils, null);
        ZoieHtmlCreator creator = new ZoieHtmlCreator();
        creator.addTable("Indexer Log Data Analysis", tab);

        // ZoieClientLogProcessor zpa =
        // new ZoieClientLogProcessor("/Users/dkumar/zoielog_first_run.log");
        // creator.addTable("Client Log Data Analysis",
        // ZoieClientLogProcessor.getTableHtml(zpa));
        // creator.addTable("Additional Client Log Data Analysis",
        // ZoieClientLogProcessor.getTableHtmlForAdditionalData(zpa));
        creator.createSimpleNormalHtmlFile(htmlOutFile, "Indexer Log Data Analysis");
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }

  }

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");

    Date date = new java.sql.Date(System.currentTimeMillis());
    String timeStamp = simpleDateFormat.format(date);
    String usageInfo = "Options";
    Options options = new Options();
    options.addOption("f", true, "File location of the file(s) to be plotted or compared");
    options.addOption("v",
                      true,
                      "Version information of the file(s) to be plotted or compared");
    options.addOption("o", true, "Name of pdf file where to generate output");
    options.addOption("w", true, "Optional Image Width");
    options.addOption("h", true, "Optional Image Height");
    options.addOption("r", false, "Override output file if it exists");
    options.addOption("i", false, "Optional create images");
    options.addOption("idir", true, "Optional directory where to create images");
    options.addOption("d", false, "Optional create data html file");
    options.addOption("dout", true, "Optional html file name where to create data output");

    CommandLine cmd = null;
    CommandLineParser parser = new PosixParser();
    try
    {
      cmd = parser.parse(options, args);
    }
    catch (Exception e)
    {
      System.err.println("Error parsing arguments");
      e.printStackTrace();
      System.exit(1);
    }

    if (!cmd.hasOption("f"))
    {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(usageInfo, options);
      System.exit(1);
    }

    String fls = cmd.getOptionValue("f");
    String[] files = fls.split(",");
    if (files.length > 1)
    {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("Currently processing of multiple input files is not supported\n"
                              + usageInfo,
                          options);
      System.exit(1);
    }
    String[] versions = null;

    if (cmd.hasOption("v"))
    {
      String ver = cmd.getOptionValue("v");
      versions = ver.split(",");
      if (versions.length != files.length)
      {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Number of versions provided should be same as number of file to be processed\n"
                                + usageInfo,
                            options);
        System.exit(1);
      }
    }
    else
    {
      versions = new String[files.length];
      for (int i = 0; i < versions.length; i++)
      {
        versions[i] = "Version " + (i + 1);
      }
    }

    int imgWidth = -1;
    int imgHeight = -1;

    if (cmd.hasOption("w"))
    {
      imgWidth = Integer.parseInt(cmd.getOptionValue("w").trim());
    }

    if (cmd.hasOption("h"))
    {
      imgHeight = Integer.parseInt(cmd.getOptionValue("h").trim());
    }

    boolean override = false;
    if (cmd.hasOption("r"))
    {
      override = true;
    }

    String outFile = null;
    String outFileName = null;

    if (cmd.hasOption("o"))
    {
      outFile = cmd.getOptionValue("o");
    }
    else
    {
      outFile = "zoie_index_data.pdf";
    }

    File existFile = new File(outFile);
    outFileName = existFile.getAbsolutePath();

    if (existFile.exists())
    {
      if (!override)
      {
        String dir = existFile.getAbsoluteFile().getParent();
        String fname = existFile.getName();
        if (fname.indexOf(".") != -1)
        {
          fname =
              fname.substring(0, fname.lastIndexOf(".")) + "_" + timeStamp
                  + fname.substring(fname.lastIndexOf("."));
        }
        outFileName = dir + File.separator + fname;
      }
    }
    if (!outFileName.endsWith(".pdf"))
    {
      outFileName = outFileName + ".pdf";
    }

    String propFileName = null;
    if (cmd.hasOption("env"))
    {
      propFileName = cmd.getOptionValue("env");
    }

    boolean createPngs = cmd.hasOption("i");
    String pngDir = null;
    if (cmd.hasOption("idir"))
    {
      pngDir = cmd.getOptionValue("idir");
    }

    boolean createHtml = false;
    if (cmd.hasOption("d"))
    {
      createHtml = true;
    }

    String htmlOutFileName = null;

    //
    if (createHtml)
    {
      String htmlOutFile = null;
      if (cmd.hasOption("dout"))
      {
        htmlOutFile = cmd.getOptionValue("dout");
      }
      else
      {
        htmlOutFile = "zoie_index_html_data.html";
      }

      File existFile1 = new File(htmlOutFile);
      htmlOutFileName = existFile1.getAbsolutePath();

      if (existFile1.exists())
      {
        if (!override)
        {
          String dir = existFile1.getAbsoluteFile().getParent();
          String fname = existFile1.getName();
          if (fname.indexOf(".") != -1)
          {
            fname =
                fname.substring(0, fname.lastIndexOf(".")) + "_" + timeStamp
                    + fname.substring(fname.lastIndexOf("."));
          }
          htmlOutFileName = dir + File.separator + fname;
        }
      }
      if (!htmlOutFileName.endsWith(".html"))
      {
        htmlOutFileName = htmlOutFileName + ".html";
      }
    }

    //
    createHtml = true;

    Map<Integer, String> indSortMap = null;
    try
    {
      ZoieIndexLogProcessor.plot(new ZoieIndexLogProcessor(files[0]),
                                 outFileName,
                                 versions[0],
                                 imgWidth,
                                 imgHeight,
                                 createPngs,
                                 pngDir,
                                 createHtml,
                                 htmlOutFileName);
      System.out.println("Ouput File Name: " + outFileName);

    }
    catch (Exception e1)
    {
      System.out.println("Exception in processing ....");
      e1.printStackTrace();
      System.exit(1);
    }

  }

  public static class ZoieIndexLogData
  {
    private long _timeSince    = -1;
    private long _count        = -1;
    private long _timeTaken    = -1;
    private long _eventFlushed = -1;
    private Date _date         = null;

    public ZoieIndexLogData(Date date,
                            long timeSince,
                            long eventFlushed,
                            long count,
                            long timeTaken)
    {
      _date = date;
      _timeSince = timeSince;
      _eventFlushed = eventFlushed;
      _count = count;
      _timeTaken = timeTaken;

    }

    private String diff(ZoieIndexLogData zd)
    {
      String result = "";
      if (zd == null)
      {
        return result;
      }
      long timeToFillRam = zd._timeSince - _timeSince;
      long tmpEventFlushed = zd._eventFlushed;
      double avgEventFlushTime =
          PerformanceManager.truncateNumber(((tmpEventFlushed) * 1000)
              / ((double) zd._timeTaken), 3);
      result =
          _dfPlot.format(zd._date) + "\t" + timeToFillRam + "\t" + avgEventFlushTime
              + "\t" + tmpEventFlushed;
      return result;

    }

    public String toString()
    {
      return _dfPlot.format(_date) + " :: " + _timeSince + " :: " + _eventFlushed
          + " :: " + _count + " :: " + _timeTaken;
    }

  }

}
