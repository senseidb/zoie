package proj.zoie.admin.client.jmx;

import java.io.Serializable;

import com.google.gwt.user.client.rpc.IsSerializable;

public class ServerInfo implements Serializable,IsSerializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int _availCPU;
	private String _serverVersion;
	private String _osVersion;
	
	public String getServerVersion() {
		return _serverVersion;
	}
	public void setServerVersion(String serverVersion) {
		_serverVersion = serverVersion;
	}
	public String getOsVersion() {
		return _osVersion;
	}
	public void setOsVersion(String osVersion) {
		_osVersion = osVersion;
	}
	public int getAvailCPU() {
		return _availCPU;
	}
	public void setAvailCPU(int availCPU) {
		_availCPU = availCPU;
	}
}
