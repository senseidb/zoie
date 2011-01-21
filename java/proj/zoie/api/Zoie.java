package proj.zoie.api;

import java.io.Serializable;

import javax.management.StandardMBean;

import org.apache.lucene.index.IndexReader;

public interface Zoie<R extends IndexReader, D, V extends ZoieVersion, VALUE extends Serializable> extends DataConsumer<D, V>, IndexReaderFactory<ZoieIndexReader<R>, VALUE>, ZoieVersionFactory<V>
{
  void start();
  void shutdown();
  StandardMBean getStandardMBean(String name);
  String[] getStandardMBeanNames();
}