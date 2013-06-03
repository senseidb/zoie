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
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

import proj.zoie.api.impl.DefaultIndexReaderMerger;
import proj.zoie.api.indexing.IndexReaderDecorator;

public abstract class ZoieIndexReader<R extends IndexReader> {
  public static final long DELETED_UID = Long.MIN_VALUE;

  public void incRef() {
    throw new UnsupportedOperationException("This reader does not implement incRef");
  }
  public void decRef() throws IOException {
    throw new UnsupportedOperationException("This reader does not implement decRef");
  }

  public int numDocs() {
    return in.numDocs();
  }

  public int maxDoc() {
    return in.maxDoc();
  }

  protected int[] _delDocIds;
  protected final IndexReaderDecorator<R> _decorator;
  protected DocIDMapper _docIDMapper;
  protected IndexReader in;

  public static <R extends IndexReader> List<R> extractDecoratedReaders(
      List<ZoieIndexReader<R>> readerList) throws IOException {
    LinkedList<R> retList = new LinkedList<R>();
    for (ZoieIndexReader<R> reader : readerList) {
      retList.addAll(reader.getDecoratedReaders());
    }
    return retList;
  }

  public static class SubReaderInfo<R extends IndexReader> {
    public final R subreader;
    public final int subdocid;

    SubReaderInfo(R subreader, int subdocid) {
      this.subreader = subreader;
      this.subdocid = subdocid;
    }
  }

  public interface SubReaderAccessor<R extends IndexReader> {
    SubReaderInfo<R> getSubReaderInfo(int docid);
  }

  public static <R extends IndexReader> SubReaderAccessor<R> getSubReaderAccessor(List<R> readerList) {
    int size = readerList.size();
    final IndexReader[] subR = new IndexReader[size];
    final int[] starts = new int[size + 1];

    int maxDoc = 0;

    int i = 0;
    for (R r : readerList) {
      starts[i] = maxDoc;
      subR[i] = r;
      maxDoc += r.maxDoc(); // compute maxDocs
      i++;
    }
    starts[size] = maxDoc;

    return new SubReaderAccessor<R>() {
      @Override
      public SubReaderInfo<R> getSubReaderInfo(int docid) {
        int subIdx = ReaderUtil.subIndex(docid, starts);
        int subdocid = docid - starts[subIdx];
        @SuppressWarnings("unchecked")
        R subreader = (R) (subR[subIdx]);
        return new SubReaderInfo<R>(subreader, subdocid);
      }
    };
  }

  public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r) throws IOException {
    return open(r, null);
  }

  public static <R extends IndexReader> ZoieIndexReader<R> open(IndexReader r,
      IndexReaderDecorator<R> decorator) throws IOException {
    return new ZoieMultiReader<R>(r, decorator);
  }

  public static <R extends IndexReader> ZoieIndexReader<R> open(Directory dir,
      IndexReaderDecorator<R> decorator) throws IOException {
    IndexReader r = DirectoryReader.open(dir);
    // load zoie reader
    try {
      return open(r, decorator);
    } catch (IOException ioe) {
      if (r != null) {
        r.close();
      }
      throw ioe;
    }
  }

  public static <R extends IndexReader, T extends IndexReader> R mergeIndexReaders(
      List<ZoieIndexReader<T>> readerList, IndexReaderMerger<R, T> merger) {
    return merger.mergeIndexReaders(readerList);
  }

  public static <T extends IndexReader> MultiReader mergeIndexReaders(
      List<ZoieIndexReader<T>> readerList) {
    return mergeIndexReaders(readerList, new DefaultIndexReaderMerger<T>());
  }

  protected ZoieIndexReader(IndexReader in, IndexReaderDecorator<R> decorator) throws IOException {
    this.in = in;
    _decorator = decorator;
    _delDocIds = null;
  }

  abstract public List<R> getDecoratedReaders() throws IOException;

  abstract public void setDelDocIds();

  abstract public void markDeletes(LongSet delDocs, LongSet deltedUIDs);

  abstract public void commitDeletes();

  public IndexReader getInnerReader() {
    return this.in;
  }

  public boolean hasDeletions() {
    int[] delSet = _delDocIds;
    if (delSet != null && delSet.length > 0) {
      return true;
    }
    return in.hasDeletions();
  }

  abstract public boolean isDeleted(int docid);

  abstract public BytesRef getStoredValue(long uid) throws IOException;

  public int[] getDelDocIds() {
    return _delDocIds;
  }

  abstract public long getUID(int docid);

  public DocIDMapper getDocIDMaper() {
    return _docIDMapper;
  }

  public void setDocIDMapper(DocIDMapper docIDMapper) {
    _docIDMapper = docIDMapper;
  }

  abstract public ZoieIndexReader<R>[] getSequentialSubReaders();

  /** TODO
   @Override
   abstract public TermDocs termDocs() throws IOException;

   @Override
   abstract public TermPositions termPositions() throws IOException;
  */

  /**
   * makes exact shallow copy of a given ZoieMultiReader
   * @return a shallow copy of a given ZoieMultiReader
   * @throws IOException
   */
  public abstract ZoieIndexReader<R> copy() throws IOException;
}
