package proj.zoie.impl.indexing;

import java.io.Serializable;
import java.util.Comparator;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.impl.DefaultDocIDMapperFactory;
import proj.zoie.impl.indexing.internal.DefaultRAMIndexFactory;
import proj.zoie.impl.indexing.internal.RAMIndexFactory;

/**
 * Configuration parameters for building a ZoieSystem.
 */
public class ZoieConfig {
  /**
   * Default real-time setting: true
   */
  public static final boolean DEFAULT_SETTING_REALTIME = true;

  /**
   * Default batch size setting: 10000
   */
  public static final int DEFAULT_SETTING_BATCHSIZE = 10000;

  /**
   * Default batch delay setting: 300000ms or 5 min
   */
  public static final int DEFAULT_SETTING_BATCHDELAY = 300000;

  /**
   * Default max batch size setting: 10000
   */
  public static final int DEFAULT_MAX_BATCH_SIZE = 10000;

  /**
   * Default version comparator
   */
  public static final Comparator<String> DEFAULT_VERSION_COMPARATOR = new DefaultVersionComparator();

  DocIDMapperFactory docidMapperFactory = null;
  Comparator<String> versionComparator = null;
  Analyzer analyzer = null;
  Similarity similarity = null;
  int batchSize;
  long batchDelay;
  boolean rtIndexing = true;
  int maxBatchSize;
  long _freshness = 10000;
  ReaderCacheFactory readercachefactory = null;
  RAMIndexFactory<?> ramIndexFactory = null;
  boolean skipBadRecord = false;

  /**
   * Default constructor. Set the size of batch and batch delay to default value
   * 10000 events and 5 minutes respectively. Indexing mode default to realtime.
   * Using the default version comparator.
   */
  public ZoieConfig() {
    this(DEFAULT_VERSION_COMPARATOR);
  }

  /**
   * Constructor. Set the size of batch and batch delay to default value
   * 10000 events and 5 minutes respectively. Indexing mode default to realtime.
   */
  public ZoieConfig(Comparator<String> versionComparator) {
    this.batchSize = DEFAULT_SETTING_BATCHSIZE;
    this.batchDelay = DEFAULT_SETTING_BATCHDELAY;
    this.rtIndexing = true;
    this.maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    this.versionComparator = versionComparator;
  }

  public boolean isSkipBadRecord() {
    return skipBadRecord;
  }

  public void setSkipBadRecord(boolean skipBadRecord) {
    this.skipBadRecord = skipBadRecord;
  }

  public DocIDMapperFactory getDocidMapperFactory() {
    return docidMapperFactory == null ? new DefaultDocIDMapperFactory() : docidMapperFactory;
  }

  public void setDocidMapperFactory(DocIDMapperFactory docidMapperFactory) {
    this.docidMapperFactory = docidMapperFactory;
  }

  public Comparator<String> getVersionComparator() {
    return versionComparator;
  }

  public void setVersionComparator(Comparator<String> versionComparator) {
    this.versionComparator = versionComparator;
  }

  public Analyzer getAnalyzer() {
    return analyzer == null ? new StandardAnalyzer(Version.LUCENE_43) : analyzer;
  }

  public void setAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  public Similarity getSimilarity() {
    return similarity == null ? new DefaultSimilarity() : similarity;
  }

  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public long getBatchDelay() {
    return batchDelay;
  }

  public void setBatchDelay(long batchDelay) {
    this.batchDelay = batchDelay;
  }

  public boolean isRtIndexing() {
    return rtIndexing;
  }

  public void setRtIndexing(boolean rtIndexing) {
    this.rtIndexing = rtIndexing;
  }

  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  public void setMaxBatchSize(int maxBatchSize) {
    this.maxBatchSize = maxBatchSize;
  }

  public long getFreshness() {
    return _freshness;
  }

  public void setFreshness(long freshness) {
    _freshness = freshness;
  }

  /**
   * @return the ReaderCacheFactory in this ZoieConfig. If the value is null, return the DefaultReaderCache Factory.
   */
  public ReaderCacheFactory getReadercachefactory() {
    if (readercachefactory == null) return DefaultReaderCache.FACTORY;
    return readercachefactory;
  }

  public void setReadercachefactory(ReaderCacheFactory readercachefactory) {
    this.readercachefactory = readercachefactory;
  }

  /**
   * @return the RAMIndexFactory in this ZoieConfig. If the value is null, return the DefaultRAMIndexFactory Factory.
   */
  @SuppressWarnings("rawtypes")
  public RAMIndexFactory<?> getRamIndexFactory() {
    if (ramIndexFactory == null) {
      return new DefaultRAMIndexFactory();
    }
    return ramIndexFactory;
  }

  public void setRamIndexFactory(RAMIndexFactory<?> ramIndexFactory) {
    this.ramIndexFactory = ramIndexFactory;
  }

  public static class DefaultVersionComparator implements Comparator<String>, Serializable {
    private static final long serialVersionUID = 1L;

    private static final Pattern _numPattern = Pattern.compile("[0-9]+");

    @Override
    public int compare(String s1, String s2) {
      if (s1 == s2) return 0;
      if (s1 == null) return -1;
      if (s2 == null) return 1;

      if (_numPattern.matcher(s1).matches() && _numPattern.matcher(s2).matches()) {
        try {
          return Long.valueOf(s1).compareTo(Long.valueOf(s2));
        } catch (Throwable t) {
        }
      }
      return s1.compareTo(s2);
    }

    public boolean equals(String s1, String s2) {
      return (compare(s1, s2) == 0);
    }
  }
}
