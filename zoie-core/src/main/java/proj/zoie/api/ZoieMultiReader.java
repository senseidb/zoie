package proj.zoie.api;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieMultiReader<R extends IndexReader> extends FilterDirectoryReader {
  private static final Logger log = Logger.getLogger(ZoieMultiReader.class.getName());
  private final Map<String, ZoieSegmentReader<R>> _readerMap;
  private final List<ZoieSegmentReader<R>> _subZoieReaders;
  private List<R> _decoratedReaders;
  private final IndexReaderDecorator<R> _decorator;
  private DocIDMapper _docIDMapper;

  public ZoieMultiReader(DirectoryReader in, IndexReaderDecorator<R> decorator) throws IOException {
    this(in, decorator, new ZoieSubReaderWrapper<R>(decorator));
  }

  @SuppressWarnings("unchecked")
  private ZoieMultiReader(DirectoryReader in, IndexReaderDecorator<R> decorator,
      ZoieSubReaderWrapper<R> wrapper) throws IOException {
    super(in, wrapper);
    _subZoieReaders = (List<ZoieSegmentReader<R>>) getSequentialSubReaders();
    _decorator = decorator;
    _readerMap = new HashMap<String, ZoieSegmentReader<R>>();
    _decoratedReaders = null;
    init();
  }

  private final AtomicLong zoieRefCounter = new AtomicLong(1);

  public void incZoieRef() {
    zoieRefCounter.incrementAndGet();
  }

  public void decZoieRef() {
    long refCount = zoieRefCounter.decrementAndGet();
    if (refCount < 0) {
      log.warn("refCount should never be lower than 0");
    }
    if (refCount == 0) {
      try {
        in.decRef();
      } catch (IOException e) {
        log.error("decZoieRef exception, ", e);
      }
    }
  }

  public int getInnerRefCount() {
    return in.getRefCount();
  }

  public DocIDMapper getDocIDMaper() {
    return _docIDMapper;
  }

  public void setDocIDMapper(DocIDMapper docIDMapper) {
    _docIDMapper = docIDMapper;
  }

  public BytesRef getStoredValue(long uid) throws IOException {
    int docid = _docIDMapper.getDocID(uid);
    if (docid < 0) return null;
    int idx = readerIndex(docid);
    if (idx < 0) return null;
    ZoieSegmentReader<R> subReader = _subZoieReaders.get(idx);
    return subReader.getStoredValue(docid);
  }

  private void init() throws IOException {
    for (ZoieSegmentReader<R> subReader : _subZoieReaders) {
      String segmentName = subReader.getSegmentName();
      _readerMap.put(segmentName, subReader);
    }

    ArrayList<R> decoratedList = new ArrayList<R>(_subZoieReaders.size());
    for (ZoieSegmentReader<R> subReader : _subZoieReaders) {
      R decoratedReader = subReader.getDecoratedReader();
      decoratedList.add(decoratedReader);
    }
    _decoratedReaders = decoratedList;
  }

  @SuppressWarnings("unchecked")
  public ZoieSegmentReader<R>[] getSubReaders() {
    return (_subZoieReaders.toArray(new ZoieSegmentReader[_subZoieReaders.size()]));
  }

  public void markDeletes(LongSet delDocs, LongSet deletedUIDs) {
    ZoieSegmentReader<R>[] subReaders = getSubReaders();
    if (subReaders != null && subReaders.length > 0) {
      for (ZoieSegmentReader<R> subReader : subReaders) {
        subReader.markDeletes(delDocs, deletedUIDs);
      }
    }
  }

  public void commitDeletes() {
    ZoieSegmentReader<R>[] subReaders = getSubReaders();
    if (subReaders != null && subReaders.length > 0) {
      for (ZoieSegmentReader<R> subReader : subReaders) {
        subReader.commitDeletes();
      }
    }
  }

  public void setDelDocIds() {
    ZoieSegmentReader<R>[] subReaders = getSubReaders();
    for (ZoieSegmentReader<R> subReader : subReaders) {
      subReader.setDelDocIds();
    }
  }

  public List<R> getDecoratedReaders() throws IOException {
    return _decoratedReaders;
  }

  public boolean isDeleted(int docid) {
    int idx = readerIndex(docid);
    ZoieSegmentReader<R> subReader = _subZoieReaders.get(idx);
    return subReader.isDeleted(docid - readerBase(idx));
  }

  public ZoieMultiReader<R> reopen() throws IOException {
    long t0 = System.currentTimeMillis();
    DirectoryReader inner = DirectoryReader.openIfChanged(in);
    if (inner == null) {
      t0 = System.currentTimeMillis() - t0;
      if (t0 > 1000) {
        log.info("reopen returns in " + t0 + "ms without change");
      } else {
        if (log.isDebugEnabled()) {
          log.debug("reopen returns in " + t0 + "ms without change");
        }
      }
      return this;
    }

    ZoieMultiReader<R> ret = new ZoieMultiReader<R>(inner, _decorator, new ZoieSubReaderWrapper<R>(
        _decorator, _readerMap));
    t0 = System.currentTimeMillis() - t0;
    if (t0 > 1000) {
      log.info("reopen returns in " + t0 + "ms with change");
    } else {
      if (log.isDebugEnabled()) {
        log.debug("reopen returns in " + t0 + "ms with change");
      }
    }
    return ret;
  }

  /**
   * makes exact shallow copy of a given ZoieMultiReader
   * @throws IOException
   */
  public ZoieMultiReader<R> copy() throws IOException {
    // increase DirectoryReader reference counter
    this.in.incRef();
    ZoieMultiReader<R> ret = new ZoieMultiReader<R>(this.in, this._decorator,
        new ZoieSubReaderWrapper<R>(this._decorator, this._readerMap));
    ret._docIDMapper = this._docIDMapper;
    return ret;
  }

  @Override
  protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
    return in;
  }

  public static class ZoieSubReaderWrapper<R extends IndexReader> extends SubReaderWrapper {
    private final IndexReaderDecorator<R> _decorator;
    private final Map<String, ZoieSegmentReader<R>> _readerMap;

    /** Constructor */
    public ZoieSubReaderWrapper(IndexReaderDecorator<R> decorator) {
      this(decorator, null);
    }

    public ZoieSubReaderWrapper(IndexReaderDecorator<R> decorator,
        Map<String, ZoieSegmentReader<R>> readerMap) {
      _decorator = decorator;
      _readerMap = readerMap;
    }

    @Override
    public AtomicReader wrap(AtomicReader reader) {
      if (!(reader instanceof SegmentReader)) {
        throw new IllegalStateException("reader not insance of " + SegmentReader.class);
      }

      try {
        if (_readerMap != null && !_readerMap.isEmpty()) {
          SegmentReader sr = (SegmentReader) reader;
          String segmentName = sr.getSegmentName();
          ZoieSegmentReader<R> zoieSegmentReader = _readerMap.get(segmentName);
          if (zoieSegmentReader != null && zoieSegmentReader.getInnerReader() == sr) {
            return new ZoieSegmentReader<R>(zoieSegmentReader, sr);
          }
        }
        return new ZoieSegmentReader<R>(reader, _decorator);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
}
