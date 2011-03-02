package proj.zoie.perf.mbean;

public interface PerfZoieServiceMBean
{
  void startPerfRun();
  void endPerfRun();
  
  void setWaitTimeMillis(int waitTime);
  int getWaitTimeMillis();
  
  int getNumSearches();
  long getAverage();
  int percentileQPS(int pct);
  int percentileLatency(int pct);
  int percentileHits(int pct);
}
