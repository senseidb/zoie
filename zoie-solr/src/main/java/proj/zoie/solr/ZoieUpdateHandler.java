package proj.zoie.solr;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieUpdateHandler extends UpdateHandler {
  private static Logger log = Logger.getLogger(ZoieUpdateHandler.class);

  private SolrCore _core;

  public ZoieUpdateHandler(SolrCore core) {
    super(core);
    _core = core;
  }

  @Override
  public int addDoc(AddUpdateCommand cmd) throws IOException {
    String id = cmd.getIndexedId(_core.getSchema());

    long zoieUid;
    try {
      zoieUid = Long.parseLong(id);
    } catch (Exception e) {
      throw new IOException("index uid must exist and of type long: " + id);
    }

    Document doc = cmd.doc;

    ZoieSystemHome zoieHome = ZoieSystemHome.getInstance(_core);
    if (zoieHome == null) {
      throw new IOException("zoie home is not setup");
    }

    Zoie<IndexReader, DocumentWithID> zoie = zoieHome.getZoieSystem();
    if (zoie == null) {
      throw new IOException("zoie is not setup");
    }
    String version = zoie.getVersion();
    DataEvent<DocumentWithID> event = new DataEvent<DocumentWithID>(
        new DocumentWithID(zoieUid, doc), version);
    try {
      zoie.consume(Arrays.asList(event));
      return 1;
    } catch (ZoieException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e.toString());
    }
  }

  @Override
  public void close() throws IOException {
    ZoieSystemHome zoieHome = ZoieSystemHome.getInstance(_core);
    if (zoieHome != null) {
      zoieHome.shutdown();
    }
  }

  private void updateReader(boolean waitForSearcher) throws IOException {
    callPostCommitCallbacks();
    Future[] waitSearcher = null;
    if (waitForSearcher) {
      waitSearcher = new Future[1];
    }
    core.getSearcher(true, false, waitSearcher);
  }

  @Override
  public void commit(CommitUpdateCommand cmd) throws IOException {
    ZoieSystemHome zoieHome = ZoieSystemHome.getInstance(_core);
    if (zoieHome != null) {
      Zoie<IndexReader, DocumentWithID> zoie = zoieHome.getZoieSystem();
      if (zoie != null) {
        try {
          zoie.flushEvents(10000);
        } catch (ZoieException e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    updateReader(cmd.waitSearcher);
  }

  @Override
  public void delete(DeleteUpdateCommand cmd) throws IOException {
    String id = cmd.id;

    long zoieUid;
    try {
      zoieUid = Long.parseLong(id);
    } catch (Exception e) {
      throw new IOException("index uid must exist and of type long: " + id);
    }

    ZoieSystemHome zoieHome = ZoieSystemHome.getInstance(_core);
    if (zoieHome == null) {
      throw new IOException("zoie home is not setup");
    }

    Zoie<IndexReader, DocumentWithID> zoie = zoieHome.getZoieSystem();
    if (zoie == null) {
      throw new IOException("zoie is not setup");
    }
    String version = zoie.getVersion();
    DataEvent<DocumentWithID> event = new DataEvent<DocumentWithID>(new DocumentWithID(zoieUid,
        true), version);
    try {
      zoie.consume(Arrays.asList(event));
    } catch (ZoieException e) {
      log.error(e.getMessage(), e);
      throw new IOException(e.toString());
    }
  }

  @Override
  public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {

    Query q = QueryParsing.parseQuery(cmd.query, schema);

    ZoieSystemHome zoieHome = ZoieSystemHome.getInstance(_core);
    if (zoieHome == null) {
      throw new IOException("zoie home is not setup");
    }

    Zoie<IndexReader, DocumentWithID> zoie = zoieHome.getZoieSystem();
    if (zoie == null) {
      throw new IOException("zoie is not setup");
    }

    final LongList delList = new LongArrayList();

    List<ZoieIndexReader<IndexReader>> readerList = null;
    IndexSearcher searcher = null;
    try {
      readerList = zoie.getIndexReaders();
      MultiReader reader = new MultiReader(readerList.toArray(new IndexReader[readerList.size()]),
          false);
      searcher = new IndexSearcher(reader);
      searcher.search(q, new Collector() {
        ZoieIndexReader<IndexReader> zoieReader = null;
        int base = 0;

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }

        @Override
        public void collect(int doc) throws IOException {
          long uid = zoieReader.getUID(doc);
          if (uid != DocIDMapper.NOT_FOUND) {
            delList.add(uid);
          }
        }

        @Override
        public void setNextReader(IndexReader reader, int base) throws IOException {
          zoieReader = (ZoieIndexReader<IndexReader>) reader;
          this.base = base;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {

        }

      });

    } finally {
      try {
        if (searcher != null) {
          searcher.close();
        }
      } finally {
        if (readerList != null) {
          zoie.returnIndexReaders(readerList);
        }
      }
    }

    if (delList.size() > 0) {
      String version = zoie.getVersion();
      ArrayList<DataEvent<DocumentWithID>> eventList = new ArrayList<DataEvent<DocumentWithID>>(
          delList.size());
      for (long val : delList) {
        eventList.add(new DataEvent<DocumentWithID>(new DocumentWithID(val, true), version));
      }
      try {
        zoie.consume(eventList);
      } catch (ZoieException e) {
        log.error(e.getMessage(), e);
        throw new IOException(e.toString());
      }
    }
  }

  @Override
  public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ZoieUpdateHandler.class
        + " doesn't support mergeIndexes.");
  }

  @Override
  public void rollback(RollbackUpdateCommand cmd) throws IOException {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ZoieUpdateHandler.class
        + " doesn't support rollback.");
  }

  // ///////////////////////////////////////////////////////////////////
  // SolrInfoMBean stuff: Statistics and Module Info
  // ///////////////////////////////////////////////////////////////////

  public String getName() {
    return ZoieUpdateHandler.class.getName();
  }

  public String getVersion() {
    return SolrCore.version;
  }

  public String getDescription() {
    return "Update handler builds on Zoie system";
  }

  public Category getCategory() {
    return Category.CORE;
  }

  public String getSourceId() {
    return "$Id$";
  }

  public String getSource() {
    return "$URL$";
  }

  public URL[] getDocs() {
    return null;
  }

  public NamedList getStatistics() {
    NamedList lst = new SimpleOrderedMap();
    return lst;
  }

}
