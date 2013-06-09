package proj.zoie.hourglass.impl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;
import proj.zoie.impl.indexing.ZoieSystem;

public class Box<R extends IndexReader, D> {
  public static final Logger log = Logger.getLogger(Box.class.getName());
  List<ZoieMultiReader<R>> _archives;
  List<ZoieSystem<R, D>> _archiveZoies;
  List<ZoieSystem<R, D>> _retiree;
  List<ZoieSystem<R, D>> _actives;
  IndexReaderDecorator<R> _decorator;

  /**
   * Copy the given lists to have immutable behavior.
   *
   * @param archives
   * @param retiree
   * @param actives
   * @param decorator
   */
  public Box(List<ZoieMultiReader<R>> archives, List<ZoieSystem<R, D>> archiveZoies,
      List<ZoieSystem<R, D>> retiree, List<ZoieSystem<R, D>> actives,
      IndexReaderDecorator<R> decorator) {
    _archives = new LinkedList<ZoieMultiReader<R>>(archives);
    _archiveZoies = new LinkedList<ZoieSystem<R, D>>(archiveZoies);
    _retiree = new LinkedList<ZoieSystem<R, D>>(retiree);
    _actives = new LinkedList<ZoieSystem<R, D>>(actives);
    _decorator = decorator;
    if (log.isDebugEnabled()) {
      for (ZoieMultiReader<R> r : _archives) {
        log.debug("archive " + r.directory() + " refCount: " + r.getInnerRefCount());
      }
    }
  }

  public void shutdown() {
    for (ZoieMultiReader<R> r : _archives) {
      r.decZoieRef();
      log.info("refCount at shutdown: " + r.getInnerRefCount() + " " + r.directory());
    }
    for (ZoieSystem<R, D> zoie : _archiveZoies) {
      zoie.shutdown();
    }
    for (ZoieSystem<R, D> zoie : _retiree) {
      zoie.shutdown();
    }
    // add the active index readers
    for (ZoieSystem<R, D> zoie : _actives) {
      while (true) {
        long flushwait = 200000L;
        try {
          zoie.flushEvents(flushwait);
          zoie.getAdminMBean().setUseCompoundFile(true);
          zoie.getAdminMBean().optimize(1);
          break;
        } catch (IOException e) {
          log.error("pre-shutdown optimization " + zoie.getAdminMBean().getIndexDir()
              + " Should investigate. But move on now.", e);
          break;
        } catch (ZoieException e) {
          if (e.getMessage().indexOf("timed out") < 0) {
            break;
          } else {
            log.info("pre-shutdown optimization " + zoie.getAdminMBean().getIndexDir()
                + " flushing processing " + flushwait + "ms elapsed");
          }
        }
      }
      zoie.shutdown();
    }
  }
}
