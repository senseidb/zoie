package proj.zoie.perf.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.deepak.util.DataAccess;
import org.deepak.util.StringParseUtility;
import org.springframework.remoting.httpinvoker.HttpInvokerProxyFactoryBean;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ZoiePerfTest
{

  private static final HttpInvokerProxyFactoryBean factoryBean              =
                                                                                new HttpInvokerProxyFactoryBean();
  private static ZoieSearchService                 _service                 = null;
  private static DataAccess                        _queries                 = null;
  private static Logger                            _logger                  = null;
  private static String                            _defaultPropertyFileName =
                                                                                "zoieperftest";
  private static String                            _defaultServiceUrl       =
                                                                                "http://localhost:8888/zoie-perf/services/ZoieService";
  private static String                            _defaultDataFile         =
                                                                                "zoiequerydata.txt";

  static
  {
    String baseName = null;
    try
    {
      String resName = System.getProperty("zoie.perf.env");

      if ((resName == null) || ("".equals(resName)))
      {
        baseName = _defaultPropertyFileName;
        System.out.println("Could not find System property named zoie.perf.env. Using default property file named "
            + _defaultPropertyFileName + ".properties");
      }
      else
      {
        if (resName.endsWith(".properties"))
        {
          resName = resName.substring(0, resName.lastIndexOf("."));
        }
        baseName = resName;
      }

    }
    catch (Exception e)
    {
      baseName = _defaultPropertyFileName;
      System.out.println("Could not find System property named facetsearch.perf.env. Using default property file named "
          + _defaultPropertyFileName + ".properties");
    }
    try
    {
      ResourceBundle resource = ResourceBundle.getBundle(baseName);
      String svcUrl = null;
      try
      {
        svcUrl =
            resource.getString("org.deepak.performance.test.zoieperftest.serviceurl");
      }
      catch (Exception ex)
      {
        System.out.println("Could not find value for service url : org.deepak.performance.test.zoieperftest.serviceurl. "
            + "Using default value of: " + _defaultServiceUrl);
        svcUrl = _defaultServiceUrl;

      }

      String dataFile = null;
      try
      {
        dataFile =
            resource.getString("org.deepak.performance.test.zoieperftest.querydatafile");
      }
      catch (Exception ex)
      {
        System.out.println("Could not find value for query data file : org.deepak.performance.test.zoieperftest.querydatafile. "
            + "Using default value of: " + _defaultDataFile);
      }

      BufferedReader br =
          new BufferedReader(new FileReader(new File(StringParseUtility.getFullPathToFile(dataFile))));
      String line = null;
      Map qMap = new HashMap();
      int cnt = 0;
      while ((line = br.readLine()) != null)
      {
        cnt++;
        String qtxt = line;
        SearchRequest req = new SearchRequest();

        if (req != null)
        {
          req.setQuery(qtxt);
          // System.out.println(req.getQuery());
          TestQuery tq = new TestQuery(req, qtxt);
          qMap.put(new Integer(cnt), tq);
        }
      }

      br.close();

      qMap = new TreeMap(qMap);
      TestQuery[] queries = new TestQuery[qMap.size()];
      System.out.println("LENGTH: " + queries.length);
      Iterator itr = qMap.keySet().iterator();
      for (int i = 0; i < queries.length; i++)
      {
        queries[i] = (TestQuery) qMap.get(itr.next());
        // System.out.println(queries[i].getRequest().getQuery());
      }
      _queries = new DataAccess(queries, DataAccess.SEQUENTIAL_ACCESS);

      factoryBean.setServiceInterface(ZoieSearchService.class);
      factoryBean.setServiceUrl(svcUrl);
      factoryBean.afterPropertiesSet();
      _service = (ZoieSearchService) (factoryBean.getObject());
    }
    catch (Exception e)
    {
      System.out.println("Exception in static method. Exiting...");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void setLogger(Logger logger)
  {
    _logger = logger;
  }

  public boolean subTransaction1(TestQuery dummy) throws Exception
  {
    boolean result = true;
    TestQuery tq = (TestQuery) _queries.next();
    long beginTime = System.currentTimeMillis();
    SearchResult sres;
    try
    {
      sres = _service.search(tq.getRequest());
    }
    catch (Exception e)
    {
      if (_logger != null)
      {
        _logger.info("FAILURE-INFO: Query Failed: " + tq.getQueryText() + " : "
            + (System.currentTimeMillis() - beginTime));
      }
      throw e;
    }
    long diff = System.currentTimeMillis() - beginTime;
    if (sres == null)
    {
      result = false;
    }
    if (result)
    {
      _logger.info("TIME-TAKEN: " + tq.getQueryText() + " : " + sres.getTotalHits()
          + " : " + diff);
    }
    else
    {
      if (_logger != null)
      {
        _logger.info("FAILURE-INFO: Query Failed: " + tq.getQueryText() + " : " + diff);
      }
    }
    return result;
  }

  public static class TestQuery
  {
    private String        _queryText = null;
    private SearchRequest _req       = null;

    public TestQuery(SearchRequest req, String qtxt)
    {
      _req = req;
      _queryText = qtxt;
    }

    public SearchRequest getRequest()
    {
      return _req;
    }

    public String getQueryText()
    {
      return _queryText;
    }
  }

}
