package proj.zoie.admin.server.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.apache.log4j.Logger;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import proj.zoie.admin.client.jmx.JMXAdminService;
import proj.zoie.admin.client.jmx.RuntimeSystemInfo;
import proj.zoie.admin.client.jmx.ServerInfo;
import proj.zoie.admin.client.jmx.ZoieServerInfo;
import proj.zoie.mbean.ZoieSystemAdminMBean;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

@SuppressWarnings("serial")
public class JMXAdminServiceImpl extends RemoteServiceServlet implements JMXAdminService {
  private static final Logger log = Logger.getLogger(JMXAdminServiceImpl.class);

	private ZoieSystemAdminMBean _zoieAdmin;
	private ThreadMXBean TMB;
	private OperatingSystemMXBean OSMB;
	private WebApplicationContext _appCtx;
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		ServletContext ctx = config.getServletContext();
		WebApplicationContext appCtx = WebApplicationContextUtils.getRequiredWebApplicationContext(ctx);
		_appCtx = appCtx;
		_zoieAdmin = (ZoieSystemAdminMBean)appCtx.getBean("zoie-system-mbean");
		TMB = ManagementFactory.getThreadMXBean();
		TMB.setThreadCpuTimeEnabled(true);
		OSMB = ManagementFactory.getOperatingSystemMXBean();
	}
	
  public ZoieServerInfo getZoieSystemInfo()
  {
    ZoieServerInfo zsi = new ZoieServerInfo();
    JMXServiceURL url;
    try
    {
      url = new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
      // "service:jmx:rmi:///jndi/rmi://localhost:9999/server");
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

      ObjectName name = new ObjectName("zoie-perf:name=zoie-system");
      extractInfo(zsi, mbsc, name);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    return zsi;
  }

  public ZoieServerInfo getDataProviderInfo()
  {
    ZoieServerInfo zsi = new ZoieServerInfo();
    JMXServiceURL url;
    try
    {
      url = new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
      // "service:jmx:rmi:///jndi/rmi://localhost:9999/server");
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

      ObjectName name = new ObjectName("zoie-perf:name=data-provider");
      extractInfo(zsi, mbsc, name);
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    return zsi;
  }
  
  public void invokeNoParam(String methodName)
  {
    JMXServiceURL url;
    try
    {
      url = new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
      // "service:jmx:rmi:///jndi/rmi://localhost:9999/server");
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

      ObjectName name = new ObjectName("zoie-perf:name=data-provider");
      mbsc.invoke(name, methodName, new Object[]{}, new String[]{});
    } catch (Exception e)
    {
      e.printStackTrace();
    }
    return;
  }

  private void extractInfo(ZoieServerInfo zsi, MBeanServerConnection mbsc,
      ObjectName name) throws InstanceNotFoundException,
      IntrospectionException, ReflectionException, IOException, MBeanException,
      AttributeNotFoundException
  {
    MBeanInfo info = mbsc.getMBeanInfo(name);
    String [] names = new String[info.getAttributes().length];
    zsi.setNames(names);
    String [] values = new String[info.getAttributes().length];
    zsi.setValues(values);
    boolean [] readable = new boolean[info.getAttributes().length];
    zsi.setReadable(readable);
    boolean [] writable = new boolean[info.getAttributes().length];
    zsi.setWritable(writable);
    int i = 0;
    for(MBeanAttributeInfo mbinfo : info.getAttributes())
    {
      names[i]  = mbinfo.getName();
      values[i] = mbsc.getAttribute(name , names[i]).toString();
      readable[i] = mbinfo.isReadable();
      writable[i] = mbinfo.isWritable();
      i++;
    }
  }

	public ServerInfo getServerInfo() {
		String serverString = getServletContext().getServerInfo();
		
		ServerInfo info = new ServerInfo();
		info.setServerVersion(serverString);
		info.setAvailCPU(OSMB.getAvailableProcessors());
		
		String osArch = OSMB.getArch();
		String osName = OSMB.getName();
		String osVer = OSMB.getVersion();
		
		StringBuilder osVersion = new StringBuilder();
		osVersion.append(osName).append(", version: ").append(osVer).append(", ").append(osArch);
		info.setOsVersion(osVersion.toString());
		return info;
	}
	
	private long[] getAllThreadCPUTimes(){
		long[] times = new long[2];
		times[0]=0L;
		times[1]=0L;
		
		long[] threadIDs = TMB.getAllThreadIds();
		for (long tid : threadIDs){
			times[0] += TMB.getThreadCpuTime(tid);
			times[1] += TMB.getThreadUserTime(tid);
		}
		return times;
	}
	
	public RuntimeSystemInfo getRuntimeSystemInfo(){
		RuntimeSystemInfo sysInfo = new RuntimeSystemInfo();
		Runtime rt = Runtime.getRuntime();
		sysInfo.setFreeMemory(rt.freeMemory());
		sysInfo.setMaxMemory(rt.maxMemory());
		sysInfo.setNumThreads(TMB.getThreadCount());
		
		long[] times = getAllThreadCPUTimes();
		sysInfo.setCpuTime(times[0]);
		sysInfo.setUserTime(times[1]);
		return sysInfo;
	}
}
