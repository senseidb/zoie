package proj.zoie.api;

import javax.management.StandardMBean;

import org.apache.lucene.index.IndexReader;

public interface Zoie<R extends IndexReader, D, V extends ZoieVersion> extends DataConsumer<D, V>, IndexReaderFactory<ZoieIndexReader<R>>
{
  void start();
  void shutdown();
  StandardMBean getStandardMBean(String name);
  String[] getStandardMBeanNames();
}