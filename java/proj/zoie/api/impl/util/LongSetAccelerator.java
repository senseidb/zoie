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

import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;

/**
 * This class, a LongSet decorator, is for accelerating look-up by having a
 * filter in front of the underlying LongSet. The filter is basically a Bloom
 * filter. The hash table size and the hash function are tuned for fast
 * calculation of hash values. The underlying LongSet should not be updated
 * while this accelerator is in use.
 * 
 * @author ymatsuda
 * 
 */
public class LongSetAccelerator implements LongSet {
	private final long[] _filter;
	private final int _mask;
	private final LongSet _set;
	private final int MIXER = 2147482951; // a prime number

	public LongSetAccelerator(LongSet set) {
		_set = set;
		int mask = set.size() / 4;
		mask |= (mask >> 1);
		mask |= (mask >> 2);
		mask |= (mask >> 4);
		mask |= (mask >> 8);
		mask |= (mask >> 16);
		_mask = mask;
		_filter = new long[mask + 1];
		LongIterator iter = set.iterator();
		while (iter.hasNext()) {
			long l = iter.nextLong();
			int h = (int) ((l >>> 32) ^ l) * MIXER;

			long bits = _filter[h & _mask];
			bits |= ((1L << (h >>> 26)));
			bits |= ((1L << ((h >> 20) & 0x3F)));
			_filter[h & _mask] = bits;
		}
	}

	public boolean contains(long val) {
		final int h = (int) (val >>> 32 ^ val) * MIXER;
		final long bits = _filter[h & _mask];

		return (bits & (1L << (h >>> 26))) != 0
				&& (bits & (1L << ((h >> 20) & 0x3F))) != 0
				&& _set.contains(val);
	}

	public boolean contains(Object o) {
		return contains(((Long) (o)).longValue());
	}

	public boolean containsAll(Collection<?> c) {
		final Iterator<?> i = c.iterator();
		int n = c.size();
		while (n-- != 0)
			if (!contains(i.next()))
				return false;

		return true;
	}

	public boolean containsAll(LongCollection c) {
		final LongIterator i = c.iterator();
		int n = c.size();
		while (n-- != 0)
			if (!contains(i.nextLong()))
				return false;
		return true;
	}

	public boolean add(long key) {
		throw new UnsupportedOperationException();
	}

	public boolean add(Long o) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends Long> c) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(LongCollection c) {
		throw new UnsupportedOperationException();
	}

	public void clear() {
		throw new UnsupportedOperationException();
	}

	public boolean equals(Object o) {
		return _set.equals(o);
	}

	public int hashCode() {
		return _set.hashCode();
	}

	public LongIterator longIterator() {
		return _set.iterator();
	}

	public boolean isEmpty() {
		return _set.isEmpty();
	}

	public LongIterator iterator() {
		return _set.iterator();
	}

	public boolean rem(long key) {
		throw new UnsupportedOperationException();
	}

	public boolean remove(long key) {
		throw new UnsupportedOperationException();
	}

	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(LongCollection c) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(LongCollection c) {
		throw new UnsupportedOperationException();
	}

	public int size() {
		return _set.size();
	}

	public Object[] toArray() {
		return _set.toArray();
	}

	public long[] toArray(long[] a) {
		return _set.toArray(a);
	}

	public <T> T[] toArray(T[] a) {
		return _set.toArray(a);
	}

	public long[] toLongArray() {
		return _set.toLongArray();
	}

	public long[] toLongArray(long[] a) {
		return _set.toLongArray(a);
	}
}
