/**
 * 
 */
package proj.zoie.api.impl;

import java.util.Arrays;

import org.apache.log4j.Logger;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.DocIDMapper.DocIDArray;

public class InRangeDocIDMapperFactory implements DocIDMapperFactory
{
  private static final Logger log = Logger.getLogger(InRangeDocIDMapperFactory.class);
  /**
   * the start UID of this contiguous partition range
   */
  private final long _partStart;
  /**
   * the max number of UIDs in this partition range
   */
  private final int _count;
  private static final int DEFAULT_RAM_COUNT_THRESHOLD = 100000;
  private final int RAM_COUNT_THRESHOLD;

  /**
   * @param start the start UID of the partition
   * @param count the number of UIDs in this partition
   */
  public InRangeDocIDMapperFactory(long start, int count)
  {
    if (start < 0 || count < 0)
    {
      throw new IllegalArgumentException("invalid range: [" + start + ","
          + count + "]");
    }
    _partStart = start;
    _count = count;
    RAM_COUNT_THRESHOLD = DEFAULT_RAM_COUNT_THRESHOLD;
  }
  /**
   * @param start the start UID of the partition
   * @param count the number of UIDs in this partition
   * @param ram_count_threshold the threshold for using array based DocIDMapper
   */
  public InRangeDocIDMapperFactory(long start, int count, int ram_count_threshold)
  {
    if (start < 0 || count < 0)
    {
      throw new IllegalArgumentException("invalid range: [" + start + ","
          + count + "]");
    }
    _partStart = start;
    _count = count;
    RAM_COUNT_THRESHOLD = ram_count_threshold;
  }

  /**
   * Get the DocIDMapper for reader and assign the DocIDMapper for each
   * sub-reader (non-Javadoc)
   * 
   * @see proj.zoie.api.DocIDMapperFactory#getDocIDMapper(proj.zoie.api.ZoieMultiReader)
   */
  public DocIDMapper<DocIDArray> getDocIDMapper(ZoieMultiReader<?> reader)
  {
    int docCount = reader.maxDoc();
    final ZoieSegmentReader<?>[] subreaders = (ZoieSegmentReader<?>[]) (reader
        .getSequentialSubReaders());
    final int[] starts = reader.getStarts();

    if (docCount > RAM_COUNT_THRESHOLD)
    { // large disk index
      final int[] docidArray = new int[_count]; // this is a mapping from local
                                                // UID (i.e., array index)
                                                // to global doc ID.
      final int[] subReaderIndex = new int[_count]; 
      Arrays.fill(docidArray, DocIDMapper.NOT_FOUND);
      Arrays.fill(subReaderIndex, DocIDMapper.NOT_FOUND);// mark to UID not exist
      for (int i = 0; i < subreaders.length; ++i)
      {
        long[] subuidarray = subreaders[i].getUIDArray();
        final int start = starts[i]; //start DOCID of this subreader
        final ZoieSegmentReader<?> subreader = subreaders[i];
        for (int k = 0; k < subuidarray.length; ++k)
        { // k is the local DOC ID for the subreader
          // subid is the global UID
          long subid = subuidarray[k]; // could be ZoieIndexReader.DELETED_UID
          if (subid != ZoieIndexReader.DELETED_UID)
          {
            int local_uid = (int) (subid - _partStart); // this is local (local to partition and not to subreader)
                                                    // relative UID index in the partition
            if ((local_uid < 0) || (local_uid >= docidArray.length))
            {
              log.error("Local UID outof range for localUID: " + local_uid + " _start: " + _partStart + " _count: " + _count + " " + (long)subid);
            }
            docidArray[local_uid] = k + start;// global DOCID
            subReaderIndex[local_uid] = i; // set the sub-reader index
          }
        }
        subreader.setDocIDMapper(new DocIDMapperSub(i, subreader, _partStart,
            docidArray, start));
      }
      return new DocIDMapperGlobal(_partStart, docidArray, subreaders, starts, subReaderIndex);
    } else
    { // small ram index
      for (int i = 0; i < subreaders.length; ++i)
      {
        ZoieSegmentReader<?> subReader = subreaders[i];
        DocIDMapper<?> mapper = subReader.getDocIDMaper();
        if (mapper == null)
        {
          mapper = new DocIDMapperImpl(subReader.getUIDArray());
        }
        subReader.setDocIDMapper(mapper);
      }
      return new DocIDMapperSmall(subreaders, starts);
    }

  }

  public static final class DocIDMapperSub implements DocIDMapper<DocIDArray>
  {
    final int max;
    final int[] uidArray;
    final int uidArrayLength;
    final long maxbdd;
    /**
     * the global DOCID of the first DOC in this subreader
     */
    final int start;
    final long _partiStart;
    final int id;
    final ZoieSegmentReader<?> subReader;

    /**
     * @param id
     * @param subreader
     * @param _partStart the first UID of the partition
     * @param uidArray the global DOC array (indexed by UID)
     * @param start the global DOCID of the first DOC in this subreader
     */
    public DocIDMapperSub(int id, ZoieSegmentReader<?> subreader, long _partStart,
        int[] uidArray, int start)
    {
      this.subReader = subreader;
      max = subreader.maxDoc() + start;
      this._partiStart = _partStart;
      this.uidArray = uidArray;
      uidArrayLength = uidArray.length;
      maxbdd = (long) uidArrayLength + _partStart;
      this.start = start;
      this.id = id;
    }

    public final int getDocID(long uid)
    {
      int mapped;
      if (uid < _partiStart || uid >= maxbdd)
      {
        return DocIDMapper.NOT_FOUND;
      } else
      {
        mapped = uidArray[(int) (uid - _partiStart)];// global DOCID
      }
      if (mapped != DocIDMapper.NOT_FOUND)
      {
        if (mapped >= max)
          return DocIDMapper.NOT_FOUND;
        return mapped - start;
      }
      return mapped; // DocIDMapper.NOT_FOUND
    }

    public DocIDArray getDocIDArray(final long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        int mapped = uidArray[(int) (uids[j] - _partiStart)];
        if (mapped != DocIDMapper.NOT_FOUND)
        {
          if (mapped >= max)
            continue;
          docids[j] = mapped - start;
        }
      }
      return ret;
    }

    public DocIDArray getDocIDArray(final int[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        int mapped = uidArray[(int) (uids[j] - _partiStart)];
        if (mapped != DocIDMapper.NOT_FOUND)
        {
          if (mapped >= max)
            continue;
          docids[j] = mapped - start;
        }
      }
      return ret;
    }

    public int quickGetDocID(long uid)
    {
      int mapped = uidArray[(int) (uid - _partiStart)];
      if (mapped != DocIDMapper.NOT_FOUND)
      {
        if (mapped >= max)
          return DocIDMapper.NOT_FOUND;
        return mapped - start;
      }
      return mapped; // DocIDMapper.NOT_FOUND
    }

    public int getReaderIndex(long uid)
    {
      return 0;
    }

    public int[] getStarts()
    {
      return new int[]{0};
    }

    public ZoieIndexReader<?>[] getSubReaders()
    {
      return new ZoieIndexReader<?>[]{subReader};
    }
  }

  public static final class DocIDMapperGlobal implements
      DocIDMapper<DocIDArray>
  {
    final long _partiStart;
    final int[] uidArray;
    final int[] subreaderindex;
    // starts have the start docID for each subreader in subreaders.
    final ZoieSegmentReader<?>[] subreaders;
    final int[] starts;

    /**
     * @param partiStart the first UID of this partition
     * @param uidArray the global DOCID array
     * @param subreaders
     * @param starts
     * @param subreaderindex
     */
    public DocIDMapperGlobal(long partiStart, int[] uidArray, ZoieSegmentReader<?>[] subreaders, int[] starts, int[] subreaderindex)
    {
      this._partiStart = partiStart;
      this.uidArray = uidArray;
      this.subreaderindex = subreaderindex;
      this.subreaders = subreaders;
      this.starts = starts;
    }

    public final int getDocID(long uid)
    {
      if (((int) uid) < _partiStart)
      {
        return DocIDMapper.NOT_FOUND;
      }
      int idx = (int) (uid - _partiStart);
      if (idx < uidArray.length)
      {
        return uidArray[idx];
      } else
      {
        return DocIDMapper.NOT_FOUND;
      }
    }

    public DocIDArray getDocIDArray(final long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        int idx = (int) (uids[j] - _partiStart);
        if (idx < uidArray.length)
        {
          docids[j] = uidArray[idx];
        }
      }
      return ret;
    }

    public DocIDArray getDocIDArray(final int[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        int idx = (int) (uids[j] - _partiStart);
        if (idx < uidArray.length)
        {
          docids[j] = uidArray[idx];
        }
      }
      return ret;
    }

    public int quickGetDocID(long uid)
    {
      int idx = (int) (uid - _partiStart);
      if (idx < uidArray.length)
      {
        return uidArray[idx];
      } else
      {
        return DocIDMapper.NOT_FOUND;
      }
    }

    public int getReaderIndex(long uid)
    {
      if (((int) uid) < _partiStart)
      {
        return -1;
      }
      int idx = (int) (uid - _partiStart);
      if (idx < uidArray.length)
      {
        return subreaderindex[idx];
      } else
      {
        return -1;
      }
    }

    public int[] getStarts()
    {
      return starts;
    }

    public ZoieIndexReader<?>[] getSubReaders()
    {
      return subreaders;
    }
  }

  public static final class DocIDMapperSmall implements DocIDMapper<DocIDArray>
  { // do the samething as DefaultDocIDMapperFactory since range does not really
    // matter here
    final ZoieSegmentReader<?>[] subreaders;
    final int[] starts;
    final DocIDMapper<?>[] mappers;
    final int bound;

    public DocIDMapperSmall(ZoieSegmentReader<?>[] subreaders, int[] starts)
    {
      this.subreaders = subreaders;
      this.starts = starts;
      mappers = new DocIDMapper[subreaders.length];
      for (int i = subreaders.length - 1; i >= 0; --i)
      {
        ZoieSegmentReader<?> subReader = subreaders[i];
        mappers[i] = subReader.getDocIDMaper();
      }
      bound = subreaders.length - 1;
    }

    public final int getDocID(long uid)
    {
      for (int i = bound; i >= 0; --i)
      {
        int docid = mappers[i].getDocID(uid);
        if (docid != DocIDMapper.NOT_FOUND)
        {
          return docid + starts[i];
        }
      }
      return DocIDMapper.NOT_FOUND;
    }

    public DocIDArray getDocIDArray(long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        for (int i = bound; i >= 0; --i)
        {
          int docid = mappers[i].quickGetDocID(uids[j]);
          if (docid != DocIDMapper.NOT_FOUND)
          {
            docids[j] = docid + starts[i];
            break;
          }
        }
      }
      return ret;
    }

    public DocIDArray getDocIDArray(int[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int[] docids = ret.docids;
      for (int j = 0; j < uids.length; j++)
      {
        for (int i = bound; i >= 0; --i)
        {
          int docid = mappers[i].quickGetDocID(uids[j]);
          if (docid != DocIDMapper.NOT_FOUND)
          {
            docids[j] = docid + starts[i];
            break;
          }
        }
      }
      return ret;
    }

    public int quickGetDocID(long uid)
    {
      for (int i = bound; i >= 0; --i)
      {
        int docid = mappers[i].quickGetDocID(uid);
        if (docid != DocIDMapper.NOT_FOUND)
        {
          return docid + starts[i];
        }
      }
      return DocIDMapper.NOT_FOUND;
    }

    public int getReaderIndex(long uid)
    {
      for (int i = bound; i >= 0; --i)
      {
        int docid = mappers[i].getDocID(uid);
        if (docid != DocIDMapper.NOT_FOUND)
        {
          return i;
        }
      }
      return -1;
    }

    public int[] getStarts()
    {
      return starts.clone();
    }

    public ZoieIndexReader<?>[] getSubReaders()
    {
      return subreaders;
    }
  }
}
