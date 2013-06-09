package proj.zoie.example.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.util.Version;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.service.api.SearchHit;
import proj.zoie.service.api.SearchRequest;
import proj.zoie.service.api.SearchResult;
import proj.zoie.service.api.ZoieSearchService;

public class ExampleZoieSearchServiceImpl<R extends IndexReader> implements ZoieSearchService {

  private static final Logger log = Logger.getLogger(ExampleZoieSearchServiceImpl.class);

  private final IndexReaderFactory<R> _idxReaderFactory;

  public ExampleZoieSearchServiceImpl(IndexReaderFactory<R> idxReaderFactory) {
    _idxReaderFactory = idxReaderFactory;
  }

  private static Map<String, String[]> convert(Document doc) {
    Map<String, String[]> map = new HashMap<String, String[]>();
    if (doc != null) {
      List<IndexableField> fields = doc.getFields();
      Iterator<IndexableField> iter = fields.iterator();
      while (iter.hasNext()) {
        IndexableField fld = iter.next();
        String fieldname = fld.name();
        map.put(fieldname, doc.getValues(fieldname));
      }
    }
    return map;
  }

  @Override
  public SearchResult search(SearchRequest req) throws ZoieException {
    String queryString = req.getQuery();
    Analyzer analyzer = _idxReaderFactory.getAnalyzer();
    QueryParser qparser = new QueryParser(Version.LUCENE_43, "content", analyzer);

    SearchResult result = new SearchResult();

    List<ZoieMultiReader<R>> readers = null;

    MultiReader multiReader = null;
    IndexSearcher searcher = null;
    try {
      Query q = null;
      if (queryString == null || queryString.length() == 0) {
        q = new MatchAllDocsQuery();
      } else {
        q = qparser.parse(queryString);
      }
      readers = _idxReaderFactory.getIndexReaders();
      multiReader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
      searcher = new IndexSearcher(multiReader);

      long start = System.currentTimeMillis();
      TopDocs docs = searcher.search(q, null, 10);
      long end = System.currentTimeMillis();

      result.setTime(end - start);
      result.setTotalDocs(multiReader.numDocs());
      result.setTotalHits(docs.totalHits);

      ScoreDoc[] scoreDocs = docs.scoreDocs;
      ArrayList<SearchHit> hitList = new ArrayList<SearchHit>(scoreDocs.length);
      for (ScoreDoc scoreDoc : scoreDocs) {
        SearchHit hit = new SearchHit();
        hit.setScore(scoreDoc.score);
        int docid = scoreDoc.doc;

        Document doc = multiReader.document(docid);
        String content = doc.get("content");

        Scorer qs = new QueryScorer(q);

        SimpleHTMLFormatter formatter = new SimpleHTMLFormatter("<span class=\"hl\">", "</span>");
        Highlighter hl = new Highlighter(formatter, qs);
        String[] fragments = hl.getBestFragments(analyzer, "content", content, 1);

        Map<String, String[]> fields = convert(doc);
        fields.put("fragment", fragments);
        hit.setFields(fields);
        hitList.add(hit);
      }

      result.setHits(hitList.toArray(new SearchHit[hitList.size()]));
      return result;
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new ZoieException(e.getMessage(), e);
    } finally {
      if (multiReader != null) {
        try {
          multiReader.close();
        } catch (IOException e) {
          log.error(e.getMessage(), e);
        }
      }
      _idxReaderFactory.returnIndexReaders(readers);
    }
  }
}
