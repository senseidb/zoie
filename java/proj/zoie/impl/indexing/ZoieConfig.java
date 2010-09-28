package proj.zoie.impl.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.impl.DefaultDocIDMapperFactory;
import proj.zoie.impl.indexing.AbstractReaderCache.ReaderCacheFactory;

/**
 * Configuration parameters for building a ZoieSystem.
 */
public class ZoieConfig
{
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

  DocIDMapperFactory docidMapperFactory = null;
  Analyzer analyzer = null;
  Similarity similarity = null;
  int batchSize;
  long batchDelay;
  boolean rtIndexing = true;
  int maxBatchSize;
  long _freshness = 10000;
  ReaderCacheFactory readercachefactory = null;

  /**
   * Default constructor. Set the size of batch and batch delay to default value
   * 10000 events and 5 minutes respectively. Indexing mode default to realtime.
   */
  public ZoieConfig()
  {
    this.batchSize = DEFAULT_SETTING_BATCHSIZE;
    this.batchDelay = DEFAULT_SETTING_BATCHDELAY;
    this.rtIndexing = true;
    this.maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
  }

  public DocIDMapperFactory getDocidMapperFactory()
  {
    return docidMapperFactory == null ? new DefaultDocIDMapperFactory()
        : docidMapperFactory;
  }

  public void setDocidMapperFactory(DocIDMapperFactory docidMapperFactory)
  {
    this.docidMapperFactory = docidMapperFactory;
  }

  public Analyzer getAnalyzer()
  {
    return analyzer == null ? new StandardAnalyzer(Version.LUCENE_CURRENT)
        : analyzer;
  }

  public void setAnalyzer(Analyzer analyzer)
  {
    this.analyzer = analyzer;
  }

  public Similarity getSimilarity()
  {
    return similarity == null ? new DefaultSimilarity() : similarity;
  }

  public void setSimilarity(Similarity similarity)
  {
    this.similarity = similarity;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public long getBatchDelay()
  {
    return batchDelay;
  }

  public void setBatchDelay(long batchDelay)
  {
    this.batchDelay = batchDelay;
  }

  public boolean isRtIndexing()
  {
    return rtIndexing;
  }

  public void setRtIndexing(boolean rtIndexing)
  {
    this.rtIndexing = rtIndexing;
  }

  public int getMaxBatchSize() {
	return maxBatchSize;
  }

  public void setMaxBatchSize(int maxBatchSize) {
	this.maxBatchSize = maxBatchSize;
  }

  public long getFreshness()
  {
    return _freshness;
  }

  public void setFreshness(long freshness)
  {
    _freshness = freshness;
  }

  /**
   * @return the ReaderCacheFactory in this ZoieConfig. If the value is null, return the DefaultReaderCache Factory.
   */
  public ReaderCacheFactory getReadercachefactory()
  {
    if (readercachefactory == null) return DefaultReaderCache.FACTORY;
    return readercachefactory;
  }

  public void setReadercachefactory(ReaderCacheFactory readercachefactory)
  {
    this.readercachefactory = readercachefactory;
  }
}
