package proj.zoie.perf.server;

import it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class MonitoredZoieService<R extends IndexReader> implements ZoieSearchService
{
  protected static int NANOS_IN_MILLI = 1000000;
  private static int SAMPLE_SIZE = 10000;
  protected int[] _latencies = new int[SAMPLE_SIZE];
  protected int[] _numHits = new int[SAMPLE_SIZE];
  protected Long2IntMap _qps = new Long2IntLinkedOpenHashMap();
  protected final AtomicInteger _numSearches = new AtomicInteger(0);

  private static final Logger log = Logger.getLogger(MonitoredZoieService.class);
	
  private final IndexReaderFactory<R> _idxReaderFactory;
  
  private long _sum = 0L;
  
  public MonitoredZoieService(IndexReaderFactory<R> idxReaderFactory)
  {
	  _idxReaderFactory = idxReaderFactory;
  }
  
  public SearchResult search(SearchRequest req) throws ZoieException
  {
	long start = System.nanoTime();
    String queryString=req.getQuery();
	Analyzer analyzer=_idxReaderFactory.getAnalyzer();
	QueryParser qparser=new QueryParser(Version.LUCENE_CURRENT,"content",analyzer);
	
	SearchResult result=new SearchResult();
	
	List<R> readers=null;
	Searcher searcher = null;
	MultiReader multiReader=null;
	try
	{
		Query q=null;
		if (queryString == null || queryString.length() ==0)
		{
			q = new MatchAllDocsQuery();
		}
		else
		{
			q = qparser.parse(queryString); 
		}
		readers=_idxReaderFactory.getIndexReaders();
		multiReader=new MultiReader(readers.toArray(new IndexReader[readers.size()]));
		searcher=new IndexSearcher(multiReader);
		TopDocs hits = searcher.search(q,10);
		result.setTotalDocs(multiReader.numDocs());
		result.setTotalHits(hits.totalHits);
		long end = System.nanoTime();
		long latency = (end-start)/NANOS_IN_MILLI;
		result.setTime(latency);
		
		int count = _numSearches.get();
		int index = count%SAMPLE_SIZE;
		
		_sum+=latency;
		
		if (count>SAMPLE_SIZE){
			_sum-=_latencies[index];
		}
//	    countQuery((end / 1000 * NANOS_IN_MILLI));
//	    now = end - now;
//	    _latencies.add( (int)(now / NANOS_IN_MILLI));
//	    _numHits.add(hits.totalHits);
		_latencies[index]=(int)latency;
		_numHits[index]=hits.totalHits;
		
		log.info("search=[query=" + req.getQuery() + "]" + ", searchResult=[numSearchResults=" + result.getTotalHits() + ";numTotalDocs=" + result.getTotalDocs() + "]" + "in " + result.getTime() + "ms"); 
	    _numSearches.incrementAndGet();
	    return result;
	}
	catch(Exception e)
	{
		log.error(e.getMessage(),e);
		throw new ZoieException(e.getMessage(),e);
	}
	finally
	{
		try{
		  if (searcher!=null){
		    try {
              searcher.close();
	       } catch (IOException e) {
			  log.error(e.getMessage(),e);
			}
		  }
		}
		finally{
		  _idxReaderFactory.returnIndexReaders(readers);
		}
	}
  }
  
  protected void countQuery(long time)
  {
    if(!_qps.containsKey(time)) { _qps.put(time, 0); }
    _qps.put(time, _qps.get(time) + 1);
  }

  public int percentileLatency(int pct)
  {
    return percentile(_latencies, pct);
  }
  
  public int percentileHits(int pct)
  {
    return percentile(_numHits, pct);
  }
  
  public int percentileQps(int pct)
  {
	int[] qps = _qps.values().toIntArray();
    Arrays.sort(qps);
    return percentile(qps, pct);
  }
  
  public int numSearches()
  {
    return _numSearches.get();
  }
  
  public long getAverage(){
	  return _sum/Math.min(_numSearches.get(), SAMPLE_SIZE);
  }
  
  
  private int percentile(int[] values, int pct)
  {
    Arrays.sort(values);
    return values[(pct/100)*values.length];
  }
}
