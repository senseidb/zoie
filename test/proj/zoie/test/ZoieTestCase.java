package proj.zoie.test;

import junit.framework.TestCase;

public class ZoieTestCase extends TestCase
{
  ZoieTestCase()
  {
    super();
    String confdir = System.getProperty("conf.dir");
    org.apache.log4j.PropertyConfigurator.configure(confdir+"/log4j.properties");
  }
  
  ZoieTestCase(String name)
  {
    super(name);
    String confdir = System.getProperty("conf.dir");
    org.apache.log4j.PropertyConfigurator.configure(confdir+"/log4j.properties");
  }
}
