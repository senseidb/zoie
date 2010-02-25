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
import java.util.Collection;
import java.util.Iterator;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;

/**
 * This class, a IntSet decorator, is for accelerating look-up by having a filter 
 * in front of the underlying IntSet. The filter is basically a Bloom filter.
 * The hash table size and the hash function are tuned for fast calculation of hash values.
 * The underlying IntSet should not be updated while this accelerator is in use.
 * 
 * @author ymatsuda
 *
 */
public class IntSetAccelerator implements IntSet
{
  private final long[] _filter;
  private final int _mask;
  private final IntSet _set;
  private final int MIXER = 2147482951; // a prime number
  
  public IntSetAccelerator(IntSet set)
  {
    _set = set;
    int mask = set.size()/4;
    mask |= (mask >> 1);
    mask |= (mask >> 2);
    mask |= (mask >> 4);
    mask |= (mask >> 8);
    mask |= (mask >> 16);
    _mask = mask;
    _filter = new long[mask+1];
    IntIterator iter = set.iterator();
    while(iter.hasNext())
    {
      int h = iter.nextInt() * MIXER;
      
      long bits = _filter[h & _mask];
      bits |= ((1L << (h >>> 26)));
      bits |= ((1L << ((h >> 20) & 0x3F)));
      _filter[h & _mask] = bits;
    }
  }
  
  public boolean contains(int val)
  {
    final int h = val * MIXER;
    final long bits = _filter[h & _mask];
    
    return (bits & (1L << (h >>> 26))) != 0 && (bits & (1L << ((h >> 20) & 0x3F))) != 0 && _set.contains(val);
  }

  public boolean contains(Object o)
  {
    return contains(((Integer)(o)).intValue());
  }

  public boolean containsAll(Collection<?> c)
  {
    final Iterator<?> i = c.iterator();
    int n = c.size();
    while(n-- != 0) if (!contains(i.next())) return false;

    return true;
  }

  public boolean containsAll(IntCollection c)
  {
    final IntIterator i = c.iterator();
    int n = c.size();
    while(n-- != 0) if (!contains(i.nextInt())) return false;
    return true;
  }

  public boolean add(int key)
  {
    throw new UnsupportedOperationException();
  }

  public boolean add(Integer o)
  {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(Collection<? extends Integer> c)
  {
    throw new UnsupportedOperationException();
  }

  public boolean addAll(IntCollection c)
  {
    throw new UnsupportedOperationException();
  }

  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  public boolean equals(Object o)
  {
    return _set.equals(o);
  }

  public int hashCode()
  {
    return _set.hashCode();
  }

  public IntIterator intIterator()
  {
    return _set.iterator();
  }

  public boolean isEmpty()
  {
    return _set.isEmpty();
  }

  public IntIterator iterator()
  {
    return _set.iterator();
  }

  public boolean rem(int key)
  {
    throw new UnsupportedOperationException();
  }

  public boolean remove(int key)
  {
    throw new UnsupportedOperationException();
  }

  public boolean remove(Object o)
  {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(Collection<?> c)
  {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(IntCollection c)
  {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(Collection<?> c)
  {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(IntCollection c)
  {
    throw new UnsupportedOperationException();
  }

  public int size()
  {
    return _set.size();
  }

  public Object[] toArray()
  {
    return _set.toArray();
  }

  public int[] toArray(int[] a)
  {
    return _set.toArray(a);
  }

  public <T> T[] toArray(T[] a)
  {
    return _set.toArray(a);
  }

  public int[] toIntArray()
  {
    return _set.toIntArray();
  }

  public int[] toIntArray(int[] a)
  {
    return _set.toIntArray(a);
  }
}
