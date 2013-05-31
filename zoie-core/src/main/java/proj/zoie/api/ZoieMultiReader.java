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

import org.apache.log4j.Logger;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieMultiReader<R extends IndexReader> extends ZoieIndexReader<R> {
  private static final Logger log = Logger.getLogger(ZoieMultiReader.class.getName());
  private final Map<String, ZoieSegmentReader<R>> _readerMap;
  private ArrayList<ZoieSegmentReader<R>> _subZoieReaders;
  private int[] _starts;
  private List<R> _decoratedReaders;

  public ZoieMultiReader(IndexReader in, IndexReaderDecorator<R> decorator) throws IOException {
    super(in, decorator);
    _readerMap = new HashMap<String, ZoieSegmentReader<R>>();
    _decoratedReaders = null;
    if (!(in instanceof DirectoryReader)) {
      throw new IllegalStateException("in not instance of " + DirectoryReader.class);
    }
    List<AtomicReaderContext> subReaderContextList = in.leaves();
    IndexReader[] subReaders = new IndexReader[subReaderContextList.size()];
    for (int i = 0; i < subReaders.length; ++i) {
      subReaders[i] = subReaderContextList.get(i).reader();
    }
    init(subReaders);
  }

  private ZoieMultiReader(IndexReader in, Object[] subReaders, IndexReaderDecorator<R> decorator)
      throws IOException {
    super(in, decorator);
    _readerMap = new HashMap<String, ZoieSegmentReader<R>>();
    _decoratedReaders = null;
    init(subReaders);
  }

  public int[] getStarts() {
    return _starts;
  }

  @Override
  public BytesRef getStoredValue(long uid) throws IOException {
    int docid = _docIDMapper.getDocID(uid);
    if (docid < 0) return null;
    int idx = readerIndex(docid);
    if (idx < 0) return null;
    ZoieSegmentReader<R> subReader = _subZoieReaders.get(idx);
    return subReader.getStoredValue(uid);
  }

  @SuppressWarnings("unchecked")
  private void init(Object[] subReaders) throws IOException {
    _subZoieReaders = new ArrayList<ZoieSegmentReader<R>>(subReaders.length);
    _starts = new int[subReaders.length + 1];
    int i = 0;
    int startCount = 0;
    for (Object subReader : subReaders) {
      ZoieSegmentReader<R> zr = null;
      if (subReader instanceof ZoieSegmentReader<?>) {
        zr = (ZoieSegmentReader<R>) subReader;
      } else if (subReader instanceof SegmentReader) {
        SegmentReader sr = (SegmentReader) subReader;
        zr = new ZoieSegmentReader<R>(sr, _decorator);
      }
      if (zr != null) {
        String segmentName = zr.getSegmentName();
        _readerMap.put(segmentName, zr);
        _subZoieReaders.add(zr);
        _starts[i] = startCount;
        i++;
        startCount += zr.maxDoc();
      } else {
        throw new IllegalStateException("subreader not instance of " + SegmentReader.class);
      }
    }

    _starts[subReaders.length] = in.maxDoc();

    ArrayList<R> decoratedList = new ArrayList<R>(_subZoieReaders.size());
    for (ZoieSegmentReader<R> subReader : _subZoieReaders) {
      R decoratedReader = subReader.getDecoratedReader();
      decoratedList.add(decoratedReader);
      subReader.incRef();
    }
    _decoratedReaders = decoratedList;
  }

  @Override
  public long getUID(int docid) {
    int idx = readerIndex(docid);
    ZoieIndexReader<R> subReader = _subZoieReaders.get(idx);
    return subReader.getUID(docid - _starts[idx]);
  }

  @Override
  public long getMinUID() {
    long uid = Long.MAX_VALUE;
    for (ZoieIndexReader<R> reader : _subZoieReaders) {
      uid = (uid > reader.getMinUID() ? reader.getMinUID() : uid);
    }
    return uid;
  }

  @Override
  public long getMaxUID() {
    long uid = Long.MIN_VALUE;
    for (ZoieIndexReader<R> reader : _subZoieReaders) {
      uid = (uid < reader.getMaxUID() ? reader.getMaxUID() : uid);
    }
    return uid;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ZoieIndexReader<R>[] getSequentialSubReaders() {
    return (_subZoieReaders.toArray(new ZoieSegmentReader[_subZoieReaders.size()]));
  }

  @Override
  public void markDeletes(LongSet delDocs, LongSet deletedUIDs) {
    ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
    if (subReaders != null && subReaders.length > 0) {
      for (int i = 0; i < subReaders.length; i++) {
        ZoieSegmentReader<R> subReader = (ZoieSegmentReader<R>) subReaders[i];
        subReader.markDeletes(delDocs, deletedUIDs);
      }
    }
  }

  @Override
  public void commitDeletes() {
    ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
    if (subReaders != null && subReaders.length > 0) {
      for (int i = 0; i < subReaders.length; i++) {
        ZoieSegmentReader<R> subReader = (ZoieSegmentReader<R>) subReaders[i];
        subReader.commitDeletes();
      }
    }
  }

  @Override
  public void setDelDocIds() {
    ZoieIndexReader<R>[] subReaders = getSequentialSubReaders();
    for (ZoieIndexReader<R> subReader : subReaders) {
      subReader.setDelDocIds();
    }
  }

  @Override
  public List<R> getDecoratedReaders() throws IOException {
    return _decoratedReaders;
  }

  @Override
  public boolean isDeleted(int docid) {
    int idx = readerIndex(docid);
    ZoieIndexReader<R> subReader = _subZoieReaders.get(idx);
    return subReader.isDeleted(docid - _starts[idx]);
  }

  private int readerIndex(int n) {
    return readerIndex(n, _starts, _starts.length);
  }

  final static int readerIndex(int n, int[] starts, int numSubReaders) { // find reader for doc n:
    int lo = 0; // search starts array
    int hi = numSubReaders - 1; // for first element less

    while (hi >= lo) {
      int mid = (lo + hi) >> 1;
      int midValue = starts[mid];
      if (n < midValue) {
        hi = mid - 1;
      } else if (n > midValue) // could be in this segment or subsequent ones
      {
        if (n < starts[mid + 1]) return mid;
        lo = mid + 1;
      } else {
        // scan to last match
        while (mid + 1 < numSubReaders && starts[mid + 1] == midValue) {
          mid++;
        }

        return mid;
      }
    }

    return hi;
  }

  /** TODO
    @Override
    public TermDocs termDocs() throws IOException {
      return new MultiZoieTermDocs(this,
          _subZoieReaders.toArray(new ZoieIndexReader<?>[_subZoieReaders.size()]), _starts);
    }

    @Override
    public TermPositions termPositions() throws IOException {
      return new MultiZoieTermPositions(this,
          _subZoieReaders.toArray(new ZoieIndexReader<?>[_subZoieReaders.size()]), _starts);
    }
  */

  @Override
  protected void doClose() throws IOException {
    try {
      decRef();
    } finally {
      for (ZoieSegmentReader<R> r : _subZoieReaders) {
        r.decRef();
      }
    }
  }

  public ZoieIndexReader<R> reopen() throws IOException {
    long t0 = System.currentTimeMillis();
    if (!(in instanceof DirectoryReader)) {
      throw new IllegalStateException("in not instance of " + DirectoryReader.class);
    }

    IndexReader inner = DirectoryReader.openIfChanged((DirectoryReader) in);
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

    List<AtomicReaderContext> subReaderContextList = inner.leaves();
    List<ZoieSegmentReader<R>> subReaderList = new ArrayList<ZoieSegmentReader<R>>(
        subReaderContextList.size());
    for (AtomicReaderContext subReaderContext : subReaderContextList) {
      if (subReaderContext.reader() instanceof SegmentReader) {
        SegmentReader sr = (SegmentReader) subReaderContext.reader();
        String segmentName = sr.getSegmentName();
        ZoieSegmentReader<R> zoieSegmentReader = _readerMap.get(segmentName);
        if (zoieSegmentReader != null) {
          zoieSegmentReader = new ZoieSegmentReader<R>(zoieSegmentReader, sr);
        } else {
          zoieSegmentReader = new ZoieSegmentReader<R>(sr, _decorator);
        }
        subReaderList.add(zoieSegmentReader);
      } else {
        throw new IllegalStateException("reader not insance of " + SegmentReader.class);
      }
    }
    ZoieIndexReader<R> ret = newInstance(inner,
      subReaderList.toArray(new IndexReader[subReaderList.size()]));
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

  protected ZoieMultiReader<R> newInstance(IndexReader inner, Object[] subReaders)
      throws IOException {
    return new ZoieMultiReader<R>(inner, subReaders, _decorator);
  }

  /**
   * makes exact shallow copy of a given ZoieMultiReader
   * @param <R>
   * @param source
   * @return
   * @throws IOException
   */
  @Override
  public ZoieMultiReader<R> copy() throws IOException {
    ArrayList<ZoieSegmentReader<R>> sourceZoieSubReaders = this._subZoieReaders;
    ArrayList<ZoieSegmentReader<R>> zoieSubReaders = new ArrayList<ZoieSegmentReader<R>>(
        this._subZoieReaders.size());
    for (ZoieSegmentReader<R> r : sourceZoieSubReaders) {
      zoieSubReaders.add(r.copy());
    }
    this.in.incRef();
    ZoieMultiReader<R> ret = this.newInstance(this.in,
      zoieSubReaders.toArray(new Object[zoieSubReaders.size()]));
    ret._docIDMapper = this._docIDMapper;
    ret._minUID = this._minUID;
    ret._maxUID = this._maxUID;
    return ret;
  }
}
