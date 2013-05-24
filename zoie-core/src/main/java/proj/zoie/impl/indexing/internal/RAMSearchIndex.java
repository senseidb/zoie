package proj.zoie.impl.indexing.internal;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.impl.ZoieMergePolicy.MergePolicyParams;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.impl.util.IndexUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class RAMSearchIndex<R extends IndexReader> extends BaseSearchIndex<R> {
  private volatile String _version;
  private final Directory _directory;
  private final File _backingdir;
  private final IndexReaderDecorator<R> _decorator;
  private static ThreadLocal<IndexWriterConfig> indexWriterConfigStorage = new ThreadLocal<IndexWriterConfig>();
  // a consistent pair of reader and deleted set
  private volatile ZoieIndexReader<R> _currentReader;
  private final MergePolicyParams _mergePolicyParams;

  public static final Logger log = Logger.getLogger(RAMSearchIndex.class);

  public RAMSearchIndex(String version, IndexReaderDecorator<R> decorator,
      SearchIndexManager<R> idxMgr, Directory ramIdxDir, File backingdir) {
    super(idxMgr, true);
    _directory = ramIdxDir;
    _backingdir = backingdir;
    _version = version;
    _decorator = decorator;
    _currentReader = null;
    _mergeScheduler = new SerialMergeScheduler();
    _mergePolicyParams = new MergePolicyParams();
    _mergePolicyParams.setNumLargeSegments(3);
    _mergePolicyParams.setMergeFactor(3);
    _mergePolicyParams.setMaxSmallSegments(4);
  }

  public void close() {
    super.close();
    if (_currentReader != null) {
      _currentReader.decZoieRef();
    }
    if (_directory != null) {
      try {
        _directory.close();
        if (_backingdir != null) FileUtil.rmDir(_backingdir);
      } catch (IOException e) {
        log.error(e);
      }
    }
  }

  public String getVersion() {
    return _version;
  }

  public void setVersion(String version) throws IOException {
    _version = version;
    synchronized (readerOpenLock) {
      readerOpenLock.notifyAll();
    }
  }

  public int getNumdocs() {
    ZoieIndexReader<R> reader = null;
    try {
      synchronized (this) {
        reader = openIndexReader();
        if (reader == null) return 0;
        reader.incZoieRef();
      }

      return reader.numDocs();
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    } finally {
      if (reader != null) reader.decZoieRef();
    }
    return 0;
  }

  @Override
  public ZoieIndexReader<R> openIndexReader() throws IOException {
    return _currentReader;
  }

  @Override
  protected IndexReader openIndexReaderForDelete() throws IOException {
    if (IndexReader.indexExists(_directory)) {
      return IndexReader.open(_directory, false);
    } else {
      return null;
    }
  }

  private ZoieIndexReader<R> openIndexReaderInternal() throws IOException {
    if (IndexReader.indexExists(_directory)) {
      IndexReader srcReader = null;
      ZoieIndexReader<R> finalReader = null;
      try {
        // for RAM indexes, just get a new index reader
        srcReader = IndexReader.open(_directory, true);
        finalReader = ZoieIndexReader.open(srcReader, _decorator);
        DocIDMapper<?> mapper = _idxMgr._docIDMapperFactory
            .getDocIDMapper((ZoieMultiReader<R>) finalReader);
        finalReader.setDocIDMapper(mapper);
        return finalReader;
      } catch (IOException ioe) {
        // if reader decoration fails, still need to close the source reader
        if (srcReader != null) {
          srcReader.close();
        }
        throw ioe;
      }
    } else {
      return null; // null indicates no index exist, following the contract
    }
  }

  public IndexWriter openIndexWriter(Analyzer analyzer, Similarity similarity) throws IOException {
    if (_indexWriter != null) return _indexWriter;

    ZoieMergePolicy mergePolicy = new ZoieMergePolicy();
    mergePolicy.setMergePolicyParams(_mergePolicyParams);
    mergePolicy.setUseCompoundFile(false);

    IndexWriterConfig config = indexWriterConfigStorage.get();
    if (config == null) {
      config = new IndexWriterConfig(Version.LUCENE_34, analyzer);
      indexWriterConfigStorage.set(config);
    }
    config.setOpenMode(OpenMode.CREATE_OR_APPEND);

    config.setMergeScheduler(_mergeScheduler);
    config.setMergePolicy(mergePolicy);

    config.setReaderPooling(false);
    if (similarity != null) {
      config.setSimilarity(similarity);
    }
    config.setRAMBufferSizeMB(3);

    IndexWriter idxWriter = new IndexWriter(_directory, config);
    _indexWriter = idxWriter;
    return idxWriter;
  }

  private final Object readerOpenLock = new Object();

  public ZoieIndexReader<R> openIndexReader(String minVersion, long timeout) throws IOException,
      TimeoutException {

    if (timeout < 0) timeout = Long.MAX_VALUE;
    if (_versionComparator.compare(minVersion, _version) <= 0) {
      return _currentReader;
    }
    long startTimer = System.currentTimeMillis();
    while (_versionComparator.compare(minVersion, _version) > 0) {
      synchronized (readerOpenLock) {
        try {
          readerOpenLock.wait(100);
        } catch (InterruptedException e) {
          // ignore
        }
      }

      long now = System.currentTimeMillis();
      if (now - startTimer >= timeout) throw new TimeoutException("timed-out, took: "
          + (now - startTimer) + " ms");
    }

    return _currentReader;

  }

  @Override
  public void refresh() throws IOException {
    synchronized (this) {
      ZoieIndexReader<R> reader = null;
      if (_currentReader == null) {
        reader = openIndexReaderInternal();
      } else {
        reader = (ZoieIndexReader<R>) _currentReader.reopen(true);
        if (reader != _currentReader) {
          DocIDMapper<?> mapper = _idxMgr._docIDMapperFactory
              .getDocIDMapper((ZoieMultiReader<R>) reader);
          reader.setDocIDMapper(mapper);
        }
      }

      if (_currentReader != reader) {
        ZoieIndexReader<R> oldReader = _currentReader;
        _currentReader = reader;
        if (oldReader != null) ((ZoieIndexReader<?>) oldReader).decZoieRef();// .decRef();
      }
      LongSet delDocs = _delDocs;
      clearDeletes();
      markDeletes(delDocs); // re-mark deletes
      commitDeletes();
    }
  }

  public int getSegmentCount() throws IOException {
    return _directory == null ? -1 : IndexUtil.getNumSegments(_directory);
  }
}
