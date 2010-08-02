package proj.zoie.test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class ZoieTestSuite extends TestSuite {
  public static List<String> allTests = Arrays.asList(new String[]{"testStreamDataProvider",
      "testRealtime","testRealtime2", "testAsyncDataConsumer", "testDelSet",
      "testIndexWithAnalyzer", "testUpdates", "testIndexSignature", "testDocIDMapper",
      "testUIDDocIdSet", "testExportImport","testDocIDMapperFactory", "testInRangeDocIDMapperFactory",
      "testThreadDelImpl",
      "testHourglassDirectoryManagerFactory"});
  public static List<Class> allClasses = Arrays.asList(new Class[]{ZoieTest.class, ZoieThreadTest.class, HourglassTest.class});

  
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
        createAndAddTest(suite, test);
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
        createAndAddTest(suite, test);
      }
    }
    return suite;
  }

  private static void createAndAddTest(TestSuite suite, String test)
  {
    for(Class clazz: allClasses)
    {
      try
      {
        Constructor constructor = clazz.getConstructor(new Class[]{String.class});
        clazz.getMethod(test);
        Test testcase = (Test) constructor.newInstance(test);
        suite.addTest(testcase);
      } catch (SecurityException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (NoSuchMethodException e)
      {
      } catch (IllegalArgumentException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InstantiationException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (IllegalAccessException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      } catch (InvocationTargetException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) {
    TestRunner.run(suite());
  }
}
