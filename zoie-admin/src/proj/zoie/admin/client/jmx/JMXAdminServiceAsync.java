package proj.zoie.admin.client.jmx;

import com.google.gwt.user.client.rpc.AsyncCallback;

public interface JMXAdminServiceAsync {
  void getZoieSystemInfo(AsyncCallback<ZoieServerInfo> callback);
  void getDataProviderInfo(AsyncCallback<ZoieServerInfo> callback);
  void invokeNoParam(String methodName, AsyncCallback<Void> callback);
  void getServerInfo(AsyncCallback<ServerInfo> callback);
	void getRuntimeSystemInfo(AsyncCallback<RuntimeSystemInfo> callback);
}
