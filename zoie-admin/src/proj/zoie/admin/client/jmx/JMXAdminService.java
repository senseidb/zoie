package proj.zoie.admin.client.jmx;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;


@RemoteServiceRelativePath("jmx")
public interface JMXAdminService extends RemoteService {
  ZoieServerInfo getZoieSystemInfo();
  ZoieServerInfo getDataProviderInfo();
  void invokeNoParam(String methodName);
  RuntimeSystemInfo getRuntimeSystemInfo();
	ServerInfo getServerInfo();
}
