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
import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A generic PriorityQueue based on heap.
 * @author Xiaoyang Gu
 * 
 * @param <T> the type of the Queue content.
 */
public class PriorityQueue<T> extends AbstractQueue<T>
{

  private final int             _capacity;
  private final T[]             _items;
  private int                   _size = 0;
  private Comparator<? super T> _comp;

  /**
   * @param capacity the maximum number of items the queue accepts
   * @param comparator a comparator that is used to order the items. 
   */
  @SuppressWarnings("unchecked")
  public PriorityQueue(int capacity, Comparator<? super T> comparator)
  {
    _capacity = capacity;
    _comp = comparator;
    _items = (T[]) new Object[capacity];// java.lang.reflect.Array.newInstance(, capacity);
  }

  /**
   * {@inheritDoc} Retrieves, but does not remove, the head of this queue. This
   * implementation returns the result of peek unless the queue is empty.
   * 
   * @see java.util.Queue#element()
   */
  public T element() throws NoSuchElementException
  {
    if (_size == 0)
      throw new NoSuchElementException("empty queue");
    return _items[0];
  }

  /**
   * Returns an iterator over the elements in this collection. There are no guarantees
   * concerning the order in which the elements are returned (unless this collection is an
   * instance of some class that provides a guarantee).
   * 
   * @see java.util.AbstractCollection#iterator()
   */
  @Override
  public Iterator<T> iterator()
  {
    return new Iterator<T>()
    {
      private int i = 0;

      public boolean hasNext()
      {
        return i < _size;
      }

      public T next() throws NoSuchElementException
      {
        if (i >= _size)
          throw new NoSuchElementException("last element reached in queue");
        return _items[i++];
      }

      public void remove()
      {
        throw new UnsupportedOperationException("not supported");
      }

    };
  }

  /**
   * Inserts the specified element into this queue, if possible. When using queues that
   * may impose insertion restrictions (for example capacity bounds), method offer is
   * generally preferable to method Collection.add, which can fail to insert an element
   * only by throwing an exception.
   * 
   * @see java.util.Queue#offer(java.lang.Object)
   */
  public boolean offer(T item)
  {
    if (_size == _capacity)
      return false;
    if (item == null)
      throw new NullPointerException();
    _items[_size] = (T) item;
    percolateUp(_size);
    _size++;
//    System.out.println("adding  to queue " + item + "  \t  " +Thread.currentThread().getClass()+Thread.currentThread().getId() );
    return true;
  }

  /**
   * Retrieves, but does not remove, the head of this queue, returning null if this queue
   * is empty.
   * 
   * @see java.util.Queue#peek()
   */
  public T peek()
  {
    if (_size == 0)
      return null;
    return _items[0];
  }

  /**
   * Retrieves and removes the head of this queue, or null if this queue is empty.
   * 
   * @see java.util.Queue#poll()
   */
  public T poll()
  {
    if (_size == 0)
      return null;
    T ret = _items[0];
    _size--;
    _items[0] = _items[_size];
    _items[_size] = null;
    if (_size > 1)
      percolateDown();
    return ret;
  }

  /**
   * Returns the number of elements in this collection.
   * 
   * @see java.util.AbstractCollection#size()
   */
  @Override
  public int size()
  {
    return _size;
  }

  private void percolateDown()
  {
    T temp = _items[0];
    int index = 0;
    while (true)
    {
      int left = (index << 1) + 1;

      int right = left + 1;
      if (right < _size)
      {
        left = _comp.compare(_items[left], _items[right]) < 0 ? left : right;
      }
      else if (left >= _size)
      {
        _items[index] = temp;
        break;
      }
      if (_comp.compare(_items[left], temp) < 0)
      {
        _items[index] = _items[left];
        index = left;
      }
      else
      {
        _items[index] = temp;
        break;
      }
    }
  }

  private void percolateUp(int index)
  {
    int i;
    T temp = _items[index];
    while ((i = ((index - 1) >> 1)) >= 0 && _comp.compare(temp, _items[i]) < 0)
    {
      _items[index] = _items[i];
      index = i;
    }
    _items[index] = temp;
  }
}
