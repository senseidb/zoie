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

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;

/**
 * @author ymatsuda
 *
 */
public class DocIDMapperImpl implements DocIDMapper
{
	private final int[] _docArray;
	  private final long[] _uidArray;
	  private final int[] _start;
	  private final long[] _filter;
	  private final int _mask;
	  private final int MIXER = 2147482951; // a prime number
	  
	  public DocIDMapperImpl(long[] uidArray)
	  {
	    int len = uidArray.length;
	    
	    int mask = len/4;
	    mask |= (mask >> 1);
	    mask |= (mask >> 2);
	    mask |= (mask >> 4);
	    mask |= (mask >> 8);
	    mask |= (mask >> 16);
	    _mask = mask;
	    
	    _filter = new long[mask+1];

	    for(long uid : uidArray)
	    {
	      if(uid != ZoieIndexReader.DELETED_UID)
	      {
	        int h = (int)((uid >>> 32) ^ uid)* MIXER;
	        
	        long bits = _filter[h & _mask];
	        bits |= ((1L << (h >>> 26)));
	        bits |= ((1L << ((h >> 20) & 0x3F)));
	        _filter[h & _mask] = bits;
	      }
	    }
	    
	    _start = new int[_mask + 1 + 1];
	    len = 0;
	    for(long uid : uidArray)
	    {
	      if(uid != ZoieIndexReader.DELETED_UID)
	      {
	        _start[((int)((uid >>> 32) ^ uid) * MIXER) & _mask]++;
	        len++;
	      }
	    }
	    int val = 0;
	    for(int i = 0; i < _start.length; i++)
	    {
	      val += _start[i];
	      _start[i] = val;
	    }
	    _start[_mask] = len;
	    
	    long[] partitionedUidArray = new long[len];
	    int[] docArray = new int[len];
	    
	    for(long uid : uidArray)
	    {
	      if(uid != ZoieIndexReader.DELETED_UID)
	      {
	        int i = --(_start[((int)((uid >>> 32) ^ uid) * MIXER) & _mask]);
	        partitionedUidArray[i] = uid;
	      }
	    }
	    
	    int s = _start[0];
	    for(int i = 1; i < _start.length; i++)
	    {
	      int e = _start[i];
	      if(s < e)
	      {
	        Arrays.sort(partitionedUidArray, s, e);
	      }
	      s = e;
	    }
	    
	    for(int docid = 0; docid < uidArray.length; docid++)
	    {
	      long uid = uidArray[docid];
	      if(uid != ZoieIndexReader.DELETED_UID)
	      {
	        final int p = ((int)((uid >>> 32) ^ uid) * MIXER) & _mask;
	        int idx = findIndex(partitionedUidArray, uid, _start[p], _start[p + 1]);
	        if(idx >= 0)
	        {
	          docArray[idx] = docid;
	        }
	      }
	    }
	    
	    _uidArray = partitionedUidArray;
	    _docArray = docArray;
	  }
	  
	  public int getDocID(final long uid)
	  {
	    final int h = (int)((uid >>> 32) ^ uid) * MIXER;
	    final int p = h & _mask;

	    // check the filter
	    final long bits = _filter[p];
	    if((bits & (1L << (h >>> 26))) == 0 || (bits & (1L << ((h >> 20) & 0x3F))) == 0) return -1; 

	    // do binary search in the partition
	    int begin = _start[p];
	    int end = _start[p + 1] - 1;
	    // we have some uids in this partition, so we assume (begin <= end)
	    while(true)
	    {
	      int mid = (begin+end) >>> 1;
	      long midval = _uidArray[mid];
	      
	      if(midval == uid) return _docArray[mid];
	      if(mid == end) return -1;
	      
	      if(midval < uid) begin = mid + 1;
	      else end = mid;
	    }
	  }
	  
	  private static final int findIndex(final long[] arr, final long uid, int begin, int end)
	  {
	    if(begin >= end) return -1;
	    end--;

	    while(true)
	    {
	      int mid = (begin+end) >>> 1; 
	      long midval = arr[mid];
	      if(midval == uid) return mid;
	      if(mid == end) return -1;
	      
	      if(midval < uid) begin = mid + 1;
	      else end = mid;
	    }
	  }
}
