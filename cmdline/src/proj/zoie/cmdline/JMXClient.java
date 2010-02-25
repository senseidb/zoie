package proj.zoie.cmdline;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import proj.zoie.mbean.DataProviderAdmin;
import proj.zoie.mbean.DataProviderAdminMBean;

public class JMXClient
{

  /**
   * @param args
   */
  public static void main(String[] args)
  {
    System.out.println("JMXClient");
    // Create an RMI connector client and
    // connect it to the RMI connector server
    //
    System.out.println("\nCreate an RMI connector client and "
        + "connect it to the RMI connector server");
    JMXServiceURL url;
    try
    {
      url = new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
      // "service:jmx:rmi:///jndi/rmi://localhost:9999/server");
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      // Create listener
      //

      // Get an MBeanServerConnection
      //
      System.out.println("\nGet an MBeanServerConnection");
      MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
      waitForEnterPressed();

      ObjectName name = new ObjectName("zoie-perf:name=zoie-system");
      MBeanInfo info = mbsc.getMBeanInfo(name);
      for(MBeanAttributeInfo mbinfo : info.getAttributes())
      {
        Object batchSize = mbsc.getAttribute(name , mbinfo.getName());
        System.out.println(mbinfo.getName() + "\t" + batchSize);
      }
      System.out.println("Data Provider");
      name = new ObjectName("zoie-perf:name=data-provider");
      info = mbsc.getMBeanInfo(name);
      System.out.println(info.getDescription());
      for(MBeanConstructorInfo i : info.getConstructors())
      {
        System.out.println(i.getDescription());
        System.out.println(i.getName());
        for(MBeanParameterInfo pi : i.getSignature())
        {
          System.out.println(pi.getDescription());
          System.out.println(pi.getName());
          System.out.println(pi.getType());
        }
      }
      System.out.println("operation");
      for(MBeanOperationInfo  oi : info.getOperations())
      {
        System.out.println(oi.getDescription());
        System.out.println(oi.getName());
        System.out.println(oi.getReturnType());
        for(MBeanParameterInfo pi : oi.getSignature())
        {
          System.out.println(pi.getDescription());
          System.out.println(pi.getName());
          System.out.println(pi.getType());
        }
      }
      System.out.println("attri ----");
      for(MBeanAttributeInfo mbinfo : info.getAttributes())
      {
        Object batchSize = mbsc.getAttribute(name , mbinfo.getName());
        System.out.println(mbinfo.getName() + "\t" + batchSize);
        System.out.println("isIs: "+mbinfo.isIs() + "\treadable: "+ mbinfo.isReadable() + "\twritable:" + mbinfo.isWritable() + "\ttype: " + mbinfo.getType());
      }
      mbsc.invoke(name, "start", new Object[]{}, (String[]) new String[]{});
      mbsc.invoke(name, "stop", new Object[]{}, (String[]) new String[]{});
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private static void waitForEnterPressed()
  {
    try
    {
      System.out.println("\nPress <Enter> to continue...");
      System.in.read();
    } catch (IOException e)
    {
      e.printStackTrace();
    }
  }
}
