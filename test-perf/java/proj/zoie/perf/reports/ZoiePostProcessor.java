package proj.zoie.perf.reports;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.deepak.performance.LoadBase;
import org.deepak.performance.MachineInfo;
import org.deepak.util.SSHClient;

import java.io.*;

public class ZoiePostProcessor
{

  public static void doPostProcessing()
  {
    String indexFileProfile =
        LoadBase.getProperty("proj.zoie.perf.report.indexfileprofile", true);
    if ((indexFileProfile == null) || ("".equals(indexFileProfile.trim())))
    {
      return;
    }

    String indexFileName = null;
    try
    {
      indexFileName = checkAndGetFile(indexFileProfile);
    }
    catch (Exception e1)
    {
      e1.printStackTrace();
    }

    String clientFileName =
        LoadBase.getProperty("org.deepak.performance.loghandlerconfig", false);
    String htmlOutFile = "zoie_perf_result.html";

    boolean overRideFiles = false;

    String tmpHtmlOut =
        LoadBase.getProperty("org.deepak.performance.perfresultoutput", true);
    if ((tmpHtmlOut != null) && (!"".equals(tmpHtmlOut.trim())))
    {
      htmlOutFile = tmpHtmlOut;
      if (!htmlOutFile.endsWith(".html"))
      {
        htmlOutFile = htmlOutFile + ".html";
      }
    }

    File outFile = new File(htmlOutFile);

    if (outFile.exists())
    {
      htmlOutFile = outFile.getName();
      if (!overRideFiles)
      {
        if (htmlOutFile.indexOf(".") != -1)
        {
          htmlOutFile =
              htmlOutFile.substring(0, htmlOutFile.lastIndexOf(".")) + "_"
                  + ZoieHtmlCreator.getTimeStamp()
                  + htmlOutFile.substring(htmlOutFile.lastIndexOf("."));
          outFile = new File(htmlOutFile);
        }
      }
    }

    String outputDir = outFile.getParentFile().getAbsolutePath();
    String subFolder = outFile.getName() + "_files";

    ZoieHtmlCreator creator = new ZoieHtmlCreator();

    try
    {
      String profileTable = ZoieTestProfileData.getTableHtml(false);
      if ((profileTable != null) && (!"".equals(profileTable.trim())))
      {
        creator.addTable("Load Test Profile".toUpperCase(), profileTable);
      }
    }
    catch (Exception e)
    {

    }

    try
    {
      String machineInfoTable =
          MachineInfo.getTableHtmlForMachines(LoadBase.getProperty("org.deepak.performance.serverprofiletobechecked",
                                                                   true));
      if ((machineInfoTable != null) && (!"".equals(machineInfoTable.trim())))
      {
        creator.addTable("Machine Profiles".toUpperCase(), machineInfoTable);
      }
    }
    catch (Exception e)
    {

    }

    try
    {
      ZoieServerLogProcessor zslp = new ZoieServerLogProcessor(indexFileName);
      String serverLogTable =
          ZoieServerLogProcessor.getTableHtml(zslp, true, outputDir, subFolder, -1, -1, 3);
      if ((serverLogTable != null) && (!"".equals(serverLogTable.trim())))
      {
        creator.addTable("Server Log Data Analysis".toUpperCase(), serverLogTable);
      }
    }
    catch (Exception e)
    {

    }

    try
    {
      ZoieClientLogProcessor zclp = new ZoieClientLogProcessor(clientFileName);
      try
      {
        String clientLogTable =
            ZoieClientLogProcessor.getTableHtml(zclp,
                                                true,
                                                outputDir,
                                                subFolder,
                                                -1,
                                                -1,
                                                3);
        if ((clientLogTable != null) && (!"".equals(clientLogTable.trim())))
        {
          creator.addTable("Client Log Data Analysis".toUpperCase(), clientLogTable);
        }
      }
      catch (Exception ex)
      {
      }

      try
      {
        String clientAddLogTable =
            ZoieClientLogProcessor.getTableHtmlForAdditionalData(zclp);
        if ((clientAddLogTable != null) && (!"".equals(clientAddLogTable.trim())))
        {
          creator.addTable("Additional Client Log Data Analysis", null);
        }
      }
      catch (Exception ex)
      {
      }

    }
    catch (Exception e)
    {

    }

    try
    {
      ZoieIndexLogProcessor zilp = new ZoieIndexLogProcessor(clientFileName);
      String indexLogTable =
          ZoieIndexLogProcessor.getTableHtml(zilp, true, outputDir, subFolder, -1, -1, 3);
      if ((indexLogTable != null) && (!"".equals(indexLogTable.trim())))
      {
        creator.addTable("Indexing Log Data Analysis".toUpperCase(), indexLogTable);
      }
    }
    catch (Exception ex)
    {

    }

    try
    {

      String clientPlatformTable =
          getLoadPlatformHtml(outputDir, subFolder, -1, -1, true, false, "center", 2);
      if ((clientPlatformTable != null) && (!"".equals(clientPlatformTable.trim())))
      {
        creator.addTable("Performance Result Charts".toUpperCase(), clientPlatformTable);
      }

    }
    catch (Exception ex)
    {

    }

    try
    {
      creator.createSimpleNormalHtmlFile(htmlOutFile, "Performance Test Results For Zoie");
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }

  }

  private static String checkAndGetFile(String str) throws Exception
  {
    String filePath = null;
    String info = str;
    if (str != null)
    {
      if (info.startsWith("["))
      {
        info = info.substring(1);
      }
      if (info.endsWith("]"))
      {
        info = info.substring(0, info.length() - 1);
      }

      Map infoMap = new HashMap();
      String[] infos = info.split(":");
      for (int j = 0; j < infos.length; j++)
      {
        String key = infos[j].substring(0, infos[j].indexOf("=")).trim().toUpperCase();
        String val = infos[j].substring(infos[j].indexOf("=") + 1).trim();
        infoMap.put(key, val);
      }

      if (infoMap.containsKey("SRC"))
      {
        String tmpSrc = ((String) infoMap.get("SRC")).trim();
        if ("remote".equalsIgnoreCase(tmpSrc.trim()))
        {
          String host = ((String) infoMap.get("HOST")).trim();
          String user = ((String) infoMap.get("USER")).trim();
          String pwd = ((String) infoMap.get("PASSWORD")).trim();
          String tmpFilePath = ((String) infoMap.get("FILEPATH")).trim();
          String tmpFileName =
              tmpFilePath.substring(tmpFilePath.lastIndexOf(File.separator)
                  + File.separator.length());
          String copyDir = null;
          if (infoMap.containsKey("COPYDIR"))
          {
            String tmpcopyDir = ((String) infoMap.get("COPYDIR")).trim();
            copyDir = new File(tmpcopyDir).getAbsolutePath();
          }
          else
          {
            copyDir = new File("").getAbsolutePath();
          }
          int port = -1;
          if (infoMap.containsKey("PORT"))
          {
            port = Integer.parseInt((String) infoMap.get("PORT"));
          }
          else
          {
            port = -1;
          }
          SSHClient ssh = null;
          if (port < 0)
          {
            ssh = new SSHClient(host, user, pwd);
          }
          else
          {
            ssh = new SSHClient(host, port, user, pwd);
          }
          ssh.login();
          ssh.copyFrom(tmpFilePath, copyDir);
          ssh.logout();
          filePath = copyDir + File.separator + tmpFileName;
        }
        else if ("local".equalsIgnoreCase(tmpSrc.trim()))
        {
          // Just return the filename here
          filePath = ((String) infoMap.get("FILEPATH")).trim();
        }
      }
    }

    return filePath;
  }

  private static String getLoadPlatformHtml(String outputDir,
                                            String subFolderForOutput,
                                            int imgWidth,
                                            int imgHeight,
                                            boolean useTimeStamp,
                                            boolean useAbsFilePath,
                                            String alignment,
                                            int numGraphsPerRow)
  {
    int number = 2;
    String tab = "";
    if (numGraphsPerRow > 0)
    {
      number = numGraphsPerRow;
    }
    List list =
        LoadBase.plotAndGetPlotHtmlTable(LoadBase.getProperty("org.deepak.performance.outputfilepath",
                                                              false),
                                         outputDir,
                                         subFolderForOutput,
                                         imgWidth,
                                         imgHeight,
                                         useTimeStamp,
                                         useAbsFilePath);

    if (list != null)
    {
      String[] parts = new String[list.size()];
      Iterator itr = list.iterator();
      for (int i = 0; i < parts.length; i++)
      {
        parts[i] = (String) itr.next();
      }
      tab = ZoieHtmlCreator.getSimpleTableHtmlStringForGraphs(parts, alignment, number);
    }

    return tab;
  }

  /**
   * @param args
   */
  public static void main(String[] args)
  {
  }

}
