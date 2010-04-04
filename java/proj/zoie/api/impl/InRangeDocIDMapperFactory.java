package proj.zoie.api.impl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.DocIDMapper.DocIDArray;
import proj.zoie.impl.indexing.ZoieSystem;

public class InRangeDocIDMapperFactory implements DocIDMapperFactory {
  private static final Logger log = Logger.getLogger(ZoieSystem.class);
	/**
	 * the start UID of this contiguous partition range
	 */
	private final long _start;
  /**
   * the max number of UIDs in this partition range
   */
	private final int _count;
	
	private static final int RAM_COUNT_THRESHOLD = 100000;
	
    public InRangeDocIDMapperFactory(long start,int count){
    	if (start<0 || count<0){
    		throw new IllegalArgumentException("invalid range: ["+start+","+count+"]");
    	}
    	_start = start;
    	_count = count;
    }
	
	/**
	 * Get the DocIDMapper for reader and assign the DocIDMapper for each sub-reader
	 *  (non-Javadoc)
	 * @see proj.zoie.api.DocIDMapperFactory#getDocIDMapper(proj.zoie.api.ZoieMultiReader)
	 */
	public DocIDMapper<DocIDArray> getDocIDMapper(ZoieMultiReader<?> reader) {
		int docCount = reader.maxDoc();
		final ZoieSegmentReader<?>[] subreaders = (ZoieSegmentReader<?>[])(reader.getSequentialSubReaders());
		final int[] starts = reader.getStarts();
		
		if (docCount > RAM_COUNT_THRESHOLD)
		{				// large disk index
			final int[] uidArray = new int[_count]; // this is a mapping from local UID (i.e., array index)
			                                        // to global doc ID.
			Arrays.fill(uidArray,DocIDMapper.NOT_FOUND);
			for (int i = 0; i < subreaders.length; ++i)
			{
				long[] subuidarray = subreaders[i].getUIDArray();
				final int start = starts[i];
				final ZoieSegmentReader<?> subreader = subreaders[i];
				for (int k=0;k<subuidarray.length;++k)
				{ // k is the local DOC ID for the subreader
				  // subid is the global UID
					long subid = subuidarray[k]; // could be ZoieIndexReader.DELETED_UID
					if (subid != ZoieIndexReader.DELETED_UID)
					{
					  int local_uid = (int)(subid-_start); // this is local relative UID index in the partition
					  if ((local_uid<0) || (local_uid>=uidArray.length))
					  {
					    log.error("Local UID outof range for localUID: " + local_uid);
					  }
					  uidArray[local_uid] = k+start;//so the global DocID is this.
					}
				}
				subreader.setDocIDMapper(new DocIDMapperSub(i, subreader, _start, uidArray, start));
			}
			return new DocIDMapperGlobal(_start, uidArray);
		}
		else{				// small ram index
			for (int i = 0; i < subreaders.length; ++i){
				ZoieSegmentReader<?> subReader = subreaders[i];
				DocIDMapper mapper = subReader.getDocIDMaper();
				if (mapper == null){
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
    final int start;
    final long _start;
    final int id;
    public DocIDMapperSub(int id, ZoieSegmentReader<?> subreader, long _start, int[] uidArray, int start)
    {
      max = subreader.maxDoc() + start;
      this._start = _start;
      this.uidArray = uidArray;
      uidArrayLength = uidArray.length;
      maxbdd = (long)uidArrayLength + _start;
      this.start = start;
      this.id = id;
    }

    public final int getDocID(long uid) {
      int mapped;
      if (uid<_start || uid>=maxbdd)
      {
        return DocIDMapper.NOT_FOUND;
      } else
      {
        mapped = uidArray[(int)(uid - _start)];
      }
      if (mapped != DocIDMapper.NOT_FOUND){
        if (mapped >= max)
          return DocIDMapper.NOT_FOUND;
        return mapped - start;
      }
      return mapped; //DocIDMapper.NOT_FOUND
    }

    public DocIDArray getDocIDArray(final long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        int mapped = uidArray[(int)(uids[j] - _start)];
        if (mapped != DocIDMapper.NOT_FOUND){
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
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        int mapped = uidArray[(int)(uids[j] - _start)];
        if (mapped != DocIDMapper.NOT_FOUND){
          if (mapped >= max)
            continue;
          docids[j] = mapped - start;
        }
      }
      return ret;
    }

    public int quickGetDocID(long uid)
    {
      int mapped = uidArray[(int)(uid - _start)];
      if (mapped != DocIDMapper.NOT_FOUND){
        if (mapped >= max)
          return DocIDMapper.NOT_FOUND;
        return mapped - start;
      }
      return mapped; //DocIDMapper.NOT_FOUND
    }
  }

	public static final class DocIDMapperGlobal implements DocIDMapper<DocIDArray>
	{
	  final long _start;
	  final int[] uidArray;
	  public DocIDMapperGlobal(long start, int[] uidArray)
	  {
	    this._start = start;
	    this.uidArray = uidArray;
	  }
	  public final int getDocID(long uid) {
	    if (((int)uid) < _start)
	    {
	      return DocIDMapper.NOT_FOUND;
	    }
	    int idx = (int)(uid-_start);
	    if (idx<uidArray.length){
	      return uidArray[idx];
	    }
	    else{
	      return DocIDMapper.NOT_FOUND;
	    }
	  }
    public DocIDArray getDocIDArray(final long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        int idx = (int)(uids[j]-_start);
        if (idx<uidArray.length){
          docids[j] = uidArray[idx];
        }
      }
      return ret;
    }
    public DocIDArray getDocIDArray(final int[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        int idx = (int)(uids[j]-_start);
        if (idx<uidArray.length){
          docids[j] = uidArray[idx];
        }
      }
      return ret;
    }
    public int quickGetDocID(long uid)
    {
      int idx = (int)(uid-_start);
      if (idx<uidArray.length){
        return uidArray[idx];
      }
      else{
        return DocIDMapper.NOT_FOUND;
      }
    }
	}

	public static final class DocIDMapperSmall implements DocIDMapper<DocIDArray>
  {        // do the samething as DefaultDocIDMapperFactory since range does not really matter here
    final ZoieSegmentReader<?>[] subreaders;
    final int[] starts;
    final DocIDMapper[] mappers;
    final int bound;
    public DocIDMapperSmall(ZoieSegmentReader<?>[] subreaders, int[] starts)
    {
      this.subreaders = subreaders;
      this.starts = starts;
      mappers = new DocIDMapper[subreaders.length];
      for (int i = subreaders.length-1; i >= 0; --i)
      {
        ZoieSegmentReader<?> subReader = subreaders[i];
        mappers[i] = subReader.getDocIDMaper();
      }
      bound = subreaders.length-1;
    }

    public final int getDocID(long uid) {
      for (int i = bound; i >= 0; --i){
        int docid = mappers[i].getDocID(uid);
        if (docid!=DocIDMapper.NOT_FOUND) {
          return docid+starts[i];
        }
      }
      return DocIDMapper.NOT_FOUND;
    }

    public DocIDArray getDocIDArray(long[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        for (int i = bound; i >= 0; --i)
        {
          int docid = mappers[i].quickGetDocID(uids[j]);
          if (docid!=DocIDMapper.NOT_FOUND) {
            docids[j] = docid+starts[i];
            break;
          }
        }
      }
      return ret;
    }

    public DocIDArray getDocIDArray(int[] uids)
    {
      DocIDArray ret = DocIDArray.newInstance(uids.length);
      int [] docids = ret.docids;
      for(int j=0;j<uids.length;j++)
      {
        for (int i = bound; i >= 0; --i)
        {
          int docid = mappers[i].quickGetDocID(uids[j]);
          if (docid!=DocIDMapper.NOT_FOUND) {
            docids[j] = docid+starts[i];
            break;
          }
        }
      }
      return ret;
    }

    public int quickGetDocID(long uid)
    {
      for (int i = bound; i >= 0; --i){
        int docid = mappers[i].quickGetDocID(uid);
        if (docid!=DocIDMapper.NOT_FOUND) {
          return docid+starts[i];
        }
      }
      return DocIDMapper.NOT_FOUND;
    }
  }
}
