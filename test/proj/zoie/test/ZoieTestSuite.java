package proj.zoie.test;

import java.util.Arrays;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class ZoieTestSuite extends TestSuite {
  public static List<String> allTests = Arrays.asList(new String[]{"testStreamDataProvider",
      "testRealtime", "testAsyncDataConsumer", "testDelSet",
      "testIndexWithAnalyzer", "testUpdates", "testIndexSignature", "testDocIDMapper",
      "testUIDDocIdSet", "testExportImport"});

  public static Test suite()
  {
    TestSuite suite=new TestSuite();
    String tests = System.getProperty("tests.to.run");
    System.out.println(tests);
    if (tests == null || tests.equals("${tests.to.run}"))
    {
      System.out.println("run all tests");
      for(String test : allTests)
      {
        System.out.println("adding test: " + test);
        suite.addTest(new ZoieTest(test));
      }
    } else
    {
      System.out.println("run tests: " + tests);
      String[] testNames = tests.split(",");
      for(String test : testNames)
      {
        if (!allTests.contains(test))
        {
          System.out.println("WARNING: " + test + " is not defined.");
          continue;
        }
        System.out.println("adding test: " + test);
        suite.addTest(new ZoieTest(test));
      }
    }
    return suite;
  }

  public static void main(String[] args) {
    TestRunner.run(suite());
  }
}
