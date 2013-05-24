package proj.zoie.api;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import proj.zoie.api.impl.ZoieContext;

/**
 * This class provides the thread pools that can understand Zoie's threadlocal context.
 * The worker thread will run the task in the ZoieContext that is the same as that of
 * the submitting thread at the job submitting time. If the ZoieContext for the submitting
 * thread has changed since the submission time before the task is run, the change in
 * the submitting thread does not affect the worker thread. If the worker thread's action
 * causes the ZoieContext to change, the change would not affect the submitting thread's
 * ZoieContext. Using the thread pools from this class will allow transparent multi-threading
 * for Zoie.
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class ZoieThreadPoolExecutor extends ThreadPoolExecutor {
  public ZoieThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  public ZoieThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
  }

  public ZoieThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  public ZoieThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime,
      TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    ZoieContext.getContext().clear();
    super.afterExecute(((ZoieRunnable) r).innerRunnable, t);
  }

  /*
   * (non-Javadoc)
   * @see java.util.concurrent.ThreadPoolExecutor#beforeExecute(java.lang.Thread,
   * java.lang.Runnable)
   */
  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    if (!(r instanceof ZoieRunnable)) throw new RuntimeException(
        "Not a properly submitted zoie job");
    ZoieContext.setContext(((ZoieRunnable) r).ctx);
    super.beforeExecute(t, ((ZoieRunnable) r).innerRunnable);
  }

  @Override
  public void execute(Runnable command) {
    super.execute(new ZoieRunnable(command));
  }

  protected static class ZoieRunnable implements Runnable {
    private final Runnable innerRunnable;
    private final ZoieContext ctx;

    /**
     * Get the current thread's ZoieContext to keep in the wrapper runnable so that
     * when the runnable is executed, the context can be set to be the same as the
     * invoking thread.
     * @param runnable
     */
    public ZoieRunnable(Runnable runnable) {
      innerRunnable = runnable;
      ctx = ZoieContext.getContext();
    }

    public ZoieContext getContext() {
      return ctx;
    }

    public void run() {
      innerRunnable.run();
    }
  }
}
