package proj.zoie.impl.indexing;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.DefaultSimilarity;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.util.Version;

import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.impl.DefaultDocIDMapperFactory;

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
	
	DocIDMapperFactory docidMapperFactory = null;
	Analyzer analyzer = null;
	Similarity similarity = null;
	int batchSize;
	long batchDelay;
	boolean rtIndexing = true;
	
	public DocIDMapperFactory getDocidMapperFactory() {
		return docidMapperFactory == null ? new DefaultDocIDMapperFactory() : docidMapperFactory;
	}
	
	public void setDocidMapperFactory(DocIDMapperFactory docidMapperFactory) {
		this.docidMapperFactory = docidMapperFactory;
	}
	
	public Analyzer getAnalyzer() {
		return analyzer == null ? new StandardAnalyzer(Version.LUCENE_CURRENT) : analyzer;
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
}
