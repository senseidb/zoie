package proj.zoie.api.impl.util;
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

import java.io.IOException;

import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class ArrayDocIdSet extends DocIdSet
{
  private final int[] _docids;
  
  public ArrayDocIdSet(int[] docids)
  {
    _docids = docids;
  }
  
  final private static int binarySearch(int[] a, int key) {
    return binarySearch(a, key, 0, a.length-1);
  }

  final private static int binarySearch(int[] a, int key, int low, int high) {
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid];
      int cmp;
      
      cmp = midVal - key;

      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else
        return mid;
    }
    return -(low + 1);
  }
  
  @Override
  public DocIdSetIterator iterator()
  {
    return new DocIdSetIterator()
    {
      int doc = -1;
      int current = -1;
      int largest = _docids.length-1;
      @Override
      public int docID()
      {
        return doc;
      }
      
      @Override
      public int nextDoc() throws IOException
      {
        if (current<_docids.length-1)
        {
          current++;
          doc = _docids[current];
          return doc;
        }
        return DocIdSetIterator.NO_MORE_DOCS;
      }
      
      @Override
      public int advance(int target) throws IOException
      {
        int idx = current < 0 ? binarySearch(_docids,target) : binarySearch(_docids,target,current,largest);
    	//  int idx = Arrays.binarySearch(_docids,target);
        if (idx < 0)
        {
          idx = -(idx+1);
          if (idx>=_docids.length) return DocIdSetIterator.NO_MORE_DOCS;
        }
        current = idx;
        doc = _docids[current];
        return doc;
      }
    };
  }
}
