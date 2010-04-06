package proj.zoie.api.impl.util;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public class MemoryManager<T>
{
  private static final Logger log = Logger.getLogger(MemoryManager.class.getName());
  private static final long SWEEP_INTERVAL = 60000; // 1min
  private static final ReentrantLock _sweepLock = new ReentrantLock();

  private static volatile long _nextSweepTime = System.currentTimeMillis();
  private static volatile int _nextSweepIndex = 0;


  private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<WeakReference<T>>> _sizeMap = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<WeakReference<T>>>();
  private final ConcurrentLinkedQueue<WeakReference<T>> _releaseQueue = new ConcurrentLinkedQueue<WeakReference<T>>();
  private Initializer<T> _initializer;
  private final Thread _cleanThread;
  public MemoryManager(Initializer<T> initializer)
  {
    this._initializer = initializer;
    _cleanThread = new Thread(new Runnable(){

      public void run()
      {
        WeakReference<T> weakbuf = null;
        T buf = null;
        while(true)
        {
          synchronized(MemoryManager.this)
          {
            try
            {
              MemoryManager.this.wait(200);
            } catch (InterruptedException e)
            {
              log.error(e);
            }
          }
          while((weakbuf = _releaseQueue.poll()) != null)
          {
            if ((buf = weakbuf.get())!=null)
            {
              ConcurrentLinkedQueue<WeakReference<T>> queue = _sizeMap.get(_initializer.size(buf));
              // buf is wrapped in WeakReference. this allows GC to reclaim the buffer memory
              _initializer.init(buf);// pre-initializing the buffer in parallel so we save time when it is requested later.
              queue.offer(new WeakReference<T>(buf));
              buf = null;
            }
          }
        }
      }});
    _cleanThread.setDaemon(true);
    _cleanThread.start();
  }

  /**
   * @return an initialized instance of type T.
   */
  public T get(int size)
  {
    size = Integer.highestOneBit(2*size);
    ConcurrentLinkedQueue<WeakReference<T>> queue = _sizeMap.get(size);
    if (queue==null)
    {
      queue =  new ConcurrentLinkedQueue<WeakReference<T>>();
      _sizeMap.putIfAbsent(size, queue);
      queue = _sizeMap.get(size);
    }
    while(true)
    {
      WeakReference<T> ref = (WeakReference<T>) queue.poll();
      if(ref != null)
      {
        T buf = ref.get();
        if(buf != null)
        {
          return buf;
        }
      }
      else
      {
        return _initializer.newInstance(size);
      }
    }
  }

  /**
   * return the instance to the manager after use
   * @param buf
   */
  public void release(T buf)
  {
    if(buf != null)
    {
      _releaseQueue.offer(new WeakReference<T>(buf));
      synchronized(MemoryManager.this)
      {
        MemoryManager.this.notifyAll();
      }
    }
  }

  public static interface Initializer<E>
  {
    public E newInstance(int size);
    public int size(E buf);
    public void init(E buf);
  }
}
