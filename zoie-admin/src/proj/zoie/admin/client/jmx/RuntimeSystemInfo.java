package proj.zoie.admin.client.jmx;

import java.io.Serializable;

import com.google.gwt.user.client.rpc.IsSerializable;

public class RuntimeSystemInfo implements Serializable,IsSerializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private long _freeMemory;
	private long _maxMemory;
	private int _numThreads;
	private long _cpuTime;
	private long _userTime;
	
	public long getFreeMemory() {
		return _freeMemory;
	}
	public void setFreeMemory(long freeMemory) {
		_freeMemory = freeMemory;
	}
	public long getMaxMemory() {
		return _maxMemory;
	}
	public void setMaxMemory(long maxMemory) {
		_maxMemory = maxMemory;
	}
	public int getNumThreads() {
		return _numThreads;
	}
	public void setNumThreads(int numThreads) {
		_numThreads = numThreads;
	}
	
	public long getCpuTime() {
		return _cpuTime;
	}
	
	public void setCpuTime(long cpuTime) {
		_cpuTime = cpuTime;
	}
	
	public long getUserTime() {
		return _userTime;
	}
	
	public void setUserTime(long userTime) {
		_userTime = userTime;
	}
	
}
