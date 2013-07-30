package proj.zoie.api.impl;

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
import java.util.Arrays;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.Bits;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieSegmentReader;

/**
 * @author ymatsuda
 *
 */
public class DocIDMapperImpl implements DocIDMapper {
  private final int[] _docArray; // the doc id of uid in _uidArray with the same index
  private final long[] _partitionedUIDArray; // partitioned uid array
  private final long[] _uidArray; // uid array
  private final int[] _start; // partition boundaries in _uidArray
  private final long[] _filter; // a helper filter to early detect false lookup
  private final int _mask; // the mask also the partition count - 1
  private static final int MIXER = 2147482951; // a prime number

  public DocIDMapperImpl(NumericDocValues uidValues, int maxDoc, Bits acceptedDocs) {
    _uidArray = new long[maxDoc];
    int len = maxDoc;

    int mask = len / 4; // 2 uids on average per partition,
                        // but we divide additional 2 for now

    // let's replace all 0's after the first 1 in the mask:
    mask |= (mask >> 1);
    mask |= (mask >> 2);
    mask |= (mask >> 4);
    mask |= (mask >> 8);
    mask |= (mask >> 16);
    _mask = mask; // all 0's replaced with 1's and we get back most of the additional divide of 2,
                  // the average per partition is a little bit more than 2 now.

    _filter = new long[mask + 1]; // one filter bits per partition.
                                  // this filter is optional, just to speed up the false lookup.

    // we will set 2 bits in this 64 bits filter per uid. since on average there are a little bit
    // more than 2 uids in each partition, so, most of the false lookup will miss at least one
    // bit. from one miss, we can tell the uid is definitely not inside the _uidArray.
    for (int i = 0; i < maxDoc; ++i) {
      if (acceptedDocs != null && !acceptedDocs.get(i)) {
        _uidArray[i] = ZoieSegmentReader.DELETED_UID;
        continue;
      }
      long uid = uidValues.get(i);
      _uidArray[i] = uid;

      // the hash function is (int)((uid >>> 32) ^ uid) * MIXER,
      // and we mod number of partions by "& _mask" (because & is much faster than mod).
      int h = (int) ((uid >>> 32) ^ uid) * MIXER;

      long bits = _filter[h & _mask];
      bits |= ((1L << (h >>> 26)));
      bits |= ((1L << ((h >> 20) & 0x3F)));
      _filter[h & _mask] = bits;
    }

    _start = new int[_mask + 1 + 1]; // we allocate 1 additinal more space for the positions

    // we fist assign the _start array with how many uid's fall into each partition:
    len = 0;
    for (int i = 0; i < maxDoc; ++i) {
      long uid = _uidArray[i];
      if (uid != ZoieSegmentReader.DELETED_UID) {
        _start[((int) ((uid >>> 32) ^ uid) * MIXER) & _mask]++;
        len++;
      }
    }

    // then, we sum them up and get all the boundaries:
    int val = 0;
    for (int i = 0; i < _start.length; i++) {
      val += _start[i];
      _start[i] = val;
    }
    _start[_mask] = len;

    // now start build the partitioned uid array and docArray:
    long[] partitionedUIDArray = new long[len];
    int[] docArray = new int[len];

    // per each uid, we will reduce the value in _start, and the new value as the index in the
    // new partitioned uid array. after all uids processed, _start[0] will be 0, and _start[1]
    // will be the previous _start[0], _start[2] will be the previous _start[1] and so on. so
    // it's like the _start array is shift one right, that's why we need an additional space
    // for the _start array:
    for (int docid = 0; docid < maxDoc; docid++) {
      long uid = _uidArray[docid];
      if (uid != ZoieSegmentReader.DELETED_UID) {
        int i = --(_start[((int) ((uid >>> 32) ^ uid) * MIXER) & _mask]);
        partitionedUIDArray[i] = uid;
      }
    }

    // sort all partitions:
    int s = _start[0];
    for (int i = 1; i < _start.length; i++) {
      int e = _start[i];
      if (s < e) {
        Arrays.sort(partitionedUIDArray, s, e);
      }
      s = e;
    }

    // assign the co-responding doc ids to the same index as the uid in the uid array
    // (note that, at first the doc id of the first uid is 0, for the second uid is 1, and so on):
    for (int docid = 0; docid < maxDoc; docid++) {
      long uid = _uidArray[docid];
      if (uid != ZoieSegmentReader.DELETED_UID) {
        final int p = ((int) ((uid >>> 32) ^ uid) * MIXER) & _mask;
        int idx = findIndex(partitionedUIDArray, uid, _start[p], _start[p + 1]);
        if (idx >= 0) {
          docArray[idx] = docid;
        }
      }
    }

    _partitionedUIDArray = partitionedUIDArray;
    _docArray = docArray;
  }

  @Override
  public int getDocID(final long uid) {
    final int h = (int) ((uid >> 32) ^ uid) * MIXER;
    final int p = h & _mask;

    // check the filter
    final long bits = _filter[p];
    if ((bits & (1L << (h >> 26))) == 0 || (bits & (1L << ((h >> 20) & 0x3F))) == 0) {
      return NOT_FOUND;
    }

    // do binary search in the partition
    int begin = _start[p];
    int end = _start[p + 1] - 1;
    // we have some uids in this partition, so we assume (begin <= end)
    while (true) {
      int mid = (begin + end) >> 1;
      long midval = _partitionedUIDArray[mid];

      if (midval == uid) {
        return _docArray[mid];
      }
      if (mid == end) {
        return NOT_FOUND;
      }
      if (midval < uid) {
        begin = mid + 1;
      } else {
        end = mid;
      }
    }
  }

  @Override
  public long[] getUIDArray() {
    return _uidArray;
  }

  private static final int findIndex(final long[] arr, final long uid, int begin, int end) {
    if (begin >= end) {
      return NOT_FOUND;
    }
    end--;

    while (true) {
      int mid = (begin + end) >> 1;
      long midval = arr[mid];
      if (midval == uid) {
        return mid;
      }
      if (mid == end) {
        return NOT_FOUND;
      }
      if (midval < uid) {
        begin = mid + 1;
      } else {
        end = mid;
      }
    }
  }

  public int[] getDocArray() {
    return _docArray;
  }
}
