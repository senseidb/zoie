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
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FilterAtomicReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class ZoieSegmentReader<R extends IndexReader> extends FilterAtomicReader {
  public static final long DELETED_UID = Long.MIN_VALUE;
  private R _decoratedReader;
  private final IndexReaderDecorator<R> _decorator;
  private IntRBTreeSet _delDocIdSet = new IntRBTreeSet();
  private int[] _currentDelDocIds = null;
  private long[] _uidArray = null;
  private DocIDMapper _docIDMapper = null;

  public static void fillDocumentID(Document doc, long id) {
    Field uidField = new NumericDocValuesField(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD, id);
    doc.add(uidField);
  }

  public ZoieSegmentReader(AtomicReader in, IndexReaderDecorator<R> decorator) throws IOException {
    super(in);
    if (!(in instanceof SegmentReader)) {
      throw new IllegalStateException("ZoieSegmentReader can only be constucted from "
          + SegmentReader.class);
    }
    init(in);
    _decorator = decorator;
    _decoratedReader = (_decorator == null ? null : _decorator.decorate(this));
  }

  private void init(AtomicReader reader) throws IOException {
    int maxDoc = reader.maxDoc();
    _uidArray = new long[maxDoc];
    NumericDocValues uidValues = reader
        .getNumericDocValues(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD);
    Bits liveDocs = reader.getLiveDocs();
    for (int i = 0; i < maxDoc; ++i) {
      if (liveDocs != null && !liveDocs.get(i)) {
        _uidArray[i] = ZoieSegmentReader.DELETED_UID;
        continue;
      }
      _uidArray[i] = uidValues.get(i);
    }
  }

  /**
   * make exact shallow copy for duplication. The decorated reader is also shallow copied.
   * @param copyFrom
   * @param innerReader
   * @throws IOException
   */
  ZoieSegmentReader(ZoieSegmentReader<R> copyFrom, AtomicReader innerReader) throws IOException {
    super(innerReader);
    _docIDMapper = copyFrom._docIDMapper;
    _decorator = copyFrom._decorator;
    _delDocIdSet = copyFrom._delDocIdSet;
    _currentDelDocIds = copyFrom._currentDelDocIds;
    _uidArray = copyFrom._uidArray;

    if (copyFrom._decorator == null) {
      _decoratedReader = null;
    } else {
      _decoratedReader = copyFrom._decorator.redecorate(copyFrom._decoratedReader, this);
    }
  }

  /**
   * makes exact shallow copy of a given ZoieSegmentReader
   * @throws IOException
   */
  public ZoieSegmentReader<R> copy() throws IOException {
    return new ZoieSegmentReader<R>(this, this.in);
  }

  public AtomicReader getInnerReader() {
    return in;
  }

  @Override
  public Bits getLiveDocs() {
    ensureOpen();
    return new Bits() {
      @Override
      public boolean get(int index) {
        int[] delSet = _currentDelDocIds;
        if (delSet != null && Arrays.binarySearch(delSet, index) >= 0) {
          return false;
        }
        Bits liveDocs = in.getLiveDocs();
        if (liveDocs == null) {
          return true;
        }
        return liveDocs.get(index);
      }

      @Override
      public int length() {
        return in.getLiveDocs().length();
      }
    };
  }

  public void markDeletes(LongSet delDocs, LongSet deletedUIDs) {
    LongIterator iter = delDocs.iterator();
    IntRBTreeSet delDocIdSet = _delDocIdSet;

    while (iter.hasNext()) {
      long uid = iter.nextLong();
      if (ZoieSegmentReader.DELETED_UID != uid) {
        int docid = _docIDMapper.getDocID(uid);
        if (docid != DocIDMapper.NOT_FOUND) {
          delDocIdSet.add(docid);
          deletedUIDs.add(uid);
        }
      }
    }
  }

  public void commitDeletes() {
    _currentDelDocIds = _delDocIdSet.toIntArray();
  }

  public R getDecoratedReader() {
    return _decoratedReader;
  }

  public BytesRef getStoredValue(int docid) throws IOException {
    Document doc = in.document(docid);
    if (doc != null) {
      return doc.getBinaryValue(AbstractZoieIndexable.DOCUMENT_STORE_FIELD);
    }
    return null;
  }

  public DocIDMapper getDocIDMapper() {
    return _docIDMapper;
  }

  public long[] getUIDArray() {
    return _uidArray;
  }

  public void setDocIDMapper(DocIDMapper docIDMapper) {
    _docIDMapper = docIDMapper;
  }

  public long getUID(int docid) {
    return _uidArray[docid];
  }

  public boolean isDeleted(int docid) {
    int[] delSet = _currentDelDocIds;
    if (delSet != null && Arrays.binarySearch(delSet, docid) >= 0) {
      return true;
    }
    Bits liveDocs = in.getLiveDocs();
    if (liveDocs == null) {
      return false;
    }
    return !liveDocs.get(docid);
  }

  public boolean isDeletedInMask(int docid) {
    int[] delSet = _currentDelDocIds;
    if (delSet != null && Arrays.binarySearch(delSet, docid) >= 0) {
      return true;
    }
    return false;
  }

  public String getSegmentName() {
    return ((SegmentReader) in).getSegmentName();
  }

  @Override
  public int numDocs() {
    if (_currentDelDocIds != null) {
      return super.numDocs() - _currentDelDocIds.length;
    } else {
      return super.numDocs();
    }
  }

  @Override
  public Object getCoreCacheKey() {
    return in.getCoreCacheKey();
  }
}
