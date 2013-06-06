package proj.zoie.perf.client;

import java.io.File;
import java.util.List;
import java.util.Random;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

import proj.zoie.api.IndexReaderFactory;

public class SearchQueryHandler implements QueryHandler<TopDocs> {

  private final Query[] _queries;
  private final IndexReaderFactory _readerFactory;
  private final Random _rand;

  public SearchQueryHandler(File queryFile, IndexReaderFactory readerFactory) throws Exception {
    _readerFactory = readerFactory;
    _rand = new Random(System.currentTimeMillis());
    List<String> queryTermList = TermFileBuilder.loadFile(queryFile);
    String[] queryTerms = queryTermList.toArray(new String[0]);
    _queries = new Query[queryTerms.length];
    for (int i = 0; i < queryTerms.length; ++i) {
      _queries[i] = new TermQuery(new Term("contents", queryTerms[i]));
    }
  }

  @Override
  public TopDocs handleQuery() throws Exception {
    List<IndexReader> readers = null;
    IndexSearcher searcher = null;
    int idx = _rand.nextInt(_queries.length);
    Query q = _queries[idx];
    try {
      readers = _readerFactory.getIndexReaders();
      MultiReader reader = new MultiReader(readers.toArray(new IndexReader[0]), false);
      searcher = new IndexSearcher(reader);
      TopDocs docs = searcher.search(q, 10);
      return docs;
    } finally {
      if (readers != null) {
        _readerFactory.returnIndexReaders(readers);
      }
    }

  }

  @Override
  public String getCurrentVersion() {
    return _readerFactory.getCurrentReaderVersion();
  }
}
