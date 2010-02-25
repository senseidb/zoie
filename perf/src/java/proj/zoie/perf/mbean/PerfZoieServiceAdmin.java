package proj.zoie.perf.mbean;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.perf.server.MonitoredZoieService;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;

public class PerfZoieServiceAdmin implements PerfZoieServiceMBean
{
  protected static int numThreads = 4;
  protected int _waitTimeMillis = 0;
  protected ExecutorService _threadPool = Executors.newFixedThreadPool(numThreads + 1);
  protected volatile boolean _perfRunStarted = false;
  
  protected volatile ZoieSystem<IndexReader,?> _zoieSystem;
  protected volatile MonitoredZoieService<?> _svc;
  protected volatile Thread _perfThread;
  
  // #queries per minute monitoring
  protected long _beginTime=0L;
  protected long _beginCount = 0;
  
  public void setWaitTimeMillis(int waitTimeMillis)
  {
    _waitTimeMillis = waitTimeMillis;
  }
  
  public int getWaitTimeMillis()
  {
    return _waitTimeMillis;
  }
  
  public void setMonitoredZoieService(MonitoredZoieService<?> svc)
  {
    _svc = svc;
  }
  
  public MonitoredZoieService<?> getMonitoredZoieService()
  {
    return _svc;
  }
  
  public void setZoieSystem(ZoieSystem<IndexReader,?> system)
  {
    _zoieSystem = system;
  }
  
  public ZoieSystem<?,?> getZoieSystem()
  {
    return _zoieSystem;
  }

  public synchronized void startPerfRun()
  {
    if(!_perfRunStarted)
    {
      _perfRunStarted = true;
      _perfThread = new Thread(new QueryDriverRunnable());
      _perfThread.start();
    }
  }
  
  public synchronized void endPerfRun()
  {
    _perfRunStarted = false;
  }

  public int percentileLatency(int pct)
  {
    return _svc.percentileLatency(pct);
  }

  public int percentileQPS(int pct)
  {
    return _svc.percentileQps(pct);
  }
  
  public int percentileHits(int pct)
  {
    return _svc.percentileHits(pct);
  }

  protected class QueryDriverRunnable implements Runnable
  {
    public void run()
    {
      List<String> queryTerms = new ArrayList<String>();
      List<ZoieIndexReader<IndexReader>> readers = null;
      TermEnum terms = null;
      try
      {
    	readers = _zoieSystem.getIndexReaders();
    	MultiReader reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]));
        int numDocs = reader.numDocs();
        terms = reader.terms();
        while(terms.next() && queryTerms.size() < 10000)
        {
          Term term = terms.term();
          int docFreq = reader.docFreq(term);
          if(docFreq * 100 >= numDocs) queryTerms.add(term.text());
        }
      }
      catch (IOException e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      finally
      {
        try
        {
          if(terms != null)
            terms.close();
        }
        catch (IOException e)
        {
          e.printStackTrace();
        }
      }
      while(_perfRunStarted)
      {
        int i = (int) (queryTerms.size() * Math.random());
        String q = queryTerms.get(i);
        final SearchRequest req = new SearchRequest();
        req.setQuery(q);
        _threadPool.submit(new Callable<SearchResult>()
        {
          public SearchResult call() throws Exception
          {
            Thread.sleep(_waitTimeMillis);
            return _svc.search(req);
          }
        });
      }
    }
  }

  public int getNumSearches()
  {
    return _svc.numSearches();
  }
  
  public void beginTiming()
  {
    _beginTime = System.nanoTime()/1000000; // in millis
    _beginCount = _svc.numSearches();
  }
  
  public long getAverage(){
	  return _svc.getAverage();
  }

  public long getQueiesPerMinute()
  {
    long currentCount = _svc.numSearches();
    long currentTime = System.nanoTime()/1000000;
    long delta = (currentCount -_beginCount)*60000;
    long duration = currentTime - _beginTime;
    if (duration < 1) return 0;
    // one minute is 60s = 60000 ms;
    long speed = delta / duration;
    return speed;
  }

}
