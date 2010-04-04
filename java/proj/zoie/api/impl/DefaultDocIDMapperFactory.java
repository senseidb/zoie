package proj.zoie.api.impl;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.DocIDMapper.DocIDArray;

public class DefaultDocIDMapperFactory implements DocIDMapperFactory {
	public DocIDMapper getDocIDMapper(ZoieMultiReader<?> multireader) {
		final ZoieSegmentReader<?>[] subreaders =(ZoieSegmentReader<?>[])(multireader.getSequentialSubReaders());
		final int[] starts = multireader.getStarts();
		for (int i = 0; i < subreaders.length; ++i){
			ZoieSegmentReader<?> subReader = subreaders[i];
			DocIDMapper mapper = subReader.getDocIDMaper();
			if (mapper == null){
				mapper = new DocIDMapperImpl(subReader.getUIDArray());
			}
			subReader.setDocIDMapper(mapper);
		}
    final DocIDMapper[] mappers = new DocIDMapper[subreaders.length];
    for(int i=0; i< subreaders.length; i++)
    {
      mappers[i] = subreaders[i].getDocIDMaper();
    }

    final int bound = subreaders.length-1;
		return new DefaultDocIDMapper(bound, mappers, starts);
		
	}
	
	public static final class DefaultDocIDMapper implements DocIDMapper<DocIDArray>
	{
	  private final int bound;
	  private final DocIDMapper[] mappers;
	  private final int[] starts;
	  public DefaultDocIDMapper(int bound, DocIDMapper[] mappers, int[] starts)
	  {
	    this.bound = bound;
	    this.mappers = mappers;
	    this.starts = starts;
	  }
	  // do the samething as DefaultDocIDMapperFactory since range does not really matter here
	  public int getDocID(long uid) {
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
      for(int j=0; j< uids.length; j++)
      {
        for (int i = bound; i >= 0; --i){
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
      for(int j=0; j< uids.length; j++)
      {
        for (int i = bound; i >= 0; --i){
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
