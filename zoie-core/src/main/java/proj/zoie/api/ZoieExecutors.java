package proj.zoie.api;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Provides the most commonly used thread pools like those in java.util.concurrent.Executors
 * that can be used for Zoie.
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class ZoieExecutors
{
  public static ExecutorService newFixedThreadPool(int nThreads)
  {
    return new ZoieThreadPoolExecutor(nThreads, nThreads, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
  }

  public static ExecutorService newFixedThreadPool(int nThreads,
      ThreadFactory threadFactory)
  {
    return new ZoieThreadPoolExecutor(nThreads, nThreads, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        threadFactory);
  }

  public static ExecutorService newCachedThreadPool()
  {
    return new ZoieThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
        TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  }

  public static ExecutorService newCachedThreadPool(ThreadFactory threadFactory)
  {
    return new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(), threadFactory);
  }
}
