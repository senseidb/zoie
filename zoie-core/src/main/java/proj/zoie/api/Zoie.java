package proj.zoie.api;

import javax.management.StandardMBean;

import org.apache.lucene.index.IndexReader;

public interface Zoie<R extends IndexReader, D> extends DataConsumer<D>, IndexReaderFactory<ZoieIndexReader<R>>
{
  void start();
  void shutdown();
  StandardMBean getStandardMBean(String name);
  String[] getStandardMBeanNames();
  void syncWithVersion(long timeInMillis, String version) throws ZoieException;
  void flushEvents(long timeout) throws ZoieException;
}