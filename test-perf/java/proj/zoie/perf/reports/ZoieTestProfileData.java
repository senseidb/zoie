package proj.zoie.perf.reports;

import org.deepak.performance.LoadBase;

public class ZoieTestProfileData
{

  public static String getTableHtml(boolean checkUnprocessed)
  {
    String table = "";
    String initThreads =
        LoadBase.getProperty("org.deepak.performance.initialthreadcount",
                             checkUnprocessed);
    int increasingThrd =
        Integer.parseInt(LoadBase.getProperty("org.deepak.performance.threadincrementcount",
                                              checkUnprocessed));
    String maxThreads =
        LoadBase.getProperty("org.deepak.performance.maxthreadcount", checkUnprocessed);

    table =
        table + getHtmlRowForProperty("Workload Type", getWorkloadType(increasingThrd));
    table = table + getHtmlRowForProperty("Starting Number Of Threads", initThreads);
    table = table + getHtmlRowForProperty("Maximum Number Of Threads", maxThreads);

    if (increasingThrd > 0)
    {
      table = table + getHtmlRowForProperty("Thread Increment Rate", "" + increasingThrd);
      table =
          table
              + getHtmlRowForProperty("Time Between Thread Increment",
                                      LoadBase.getProperty("org.deepak.performance.threadincrementtimeinsec",
                                                           checkUnprocessed)
                                          + " (s)");
    }

    double testDuration =
        Double.parseDouble(LoadBase.getProperty("org.deepak.performance.runtimeinminute",
                                                checkUnprocessed));

    if (testDuration < 0)
    {
      table = table + getHtmlRowForProperty("Test Duration", "" + "UNLIMITED");
    }
    else
    {
      table =
          table + getHtmlRowForProperty("Test Duration", "" + testDuration + " (mins)");
    }

    String transInfo =
        LoadBase.getProperty("org.deepak.performance.transactioninfo", checkUnprocessed);
    String[] transactions = transInfo.split(",");

    table =
        table
            + getHtmlRowForProperty("Number Of Functionalities Being Tested", ""
                + transactions.length);

    String transDetails = "";
    for (int i = 0; i < transactions.length; i++)
    {
      if (i == 0)
      {
        transDetails = transDetails + "subTransaction" + transactions[i].trim();
      }
      else
      {
        transDetails = transDetails + ", " + "subTransaction" + transactions[i].trim();
      }

    }
    table = table + getHtmlRowForProperty("Subtransactions Being Executed", transDetails);
    table =
        table
            + getHtmlRowForProperty("Functionalities Being Simulated",
                                    LoadBase.getProperty("org.deepak.performance.functionalityinfo",
                                                         checkUnprocessed));
    table =
        table
            + getHtmlRowForProperty("Think Time For Functionalities",
                                    LoadBase.getProperty("org.deepak.performance.thinktimeinfo",
                                                         checkUnprocessed)
                                        + " (s)");

    table =
        ZoieHtmlCreator.getSimpleTableHtmlString(new String[] { "Property", "Value" },
                                                 table);
    return table;

  }

  public static String getHtmlRowForProperty(String key, String value)
  {
    String table = "";
    table = table + "<tr valign=\"top\">\n";
    table = table + "<td> " + key + " </td>\n";
    table = table + "<td> " + value + " </td>\n";
    table = table + "</tr>\n";
    return table;
  }

  public static String getWorkloadType(int type)
  {
    if (type > 0)
    {
      return "INCREASING";
    }
    else
    {
      return "STEADY STATE";
    }
  }

}
