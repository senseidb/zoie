package proj.zoie.api.impl;

import java.util.Arrays;

import org.apache.log4j.Logger;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.impl.indexing.ZoieSystem;

public class InRangeDocIDMapperFactory implements DocIDMapperFactory {
  private static final Logger log = Logger.getLogger(ZoieSystem.class);
	private final long _start;
	private final int _count;
	
	private static final int RAM_COUNT_THRESHOLD = 100000;
	
    public InRangeDocIDMapperFactory(long start,int count){
    	if (start<0 || count<0){
    		throw new IllegalArgumentException("invalid range: ["+start+","+count+"]");
    	}
    	_start = start;
    	_count = count;
    }
	
	public DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader) {
		int docCount = reader.maxDoc();
		final ZoieSegmentReader<?>[] subreaders = (ZoieSegmentReader<?>[])(reader.getSequentialSubReaders());
		final int[] starts = reader.getStarts();
		
		if (docCount > RAM_COUNT_THRESHOLD){				// large disk index
			final int[] uidArray = new int[_count]; // this is a mapping from local UID (i.e., array index)
			                                        // to global doc ID.
			Arrays.fill(uidArray,DocIDMapper.NOT_FOUND);
			for (int i = 0; i < subreaders.length; ++i){
				long[] subuidarray = subreaders[i].getUIDArray();
				final int start = starts[i];
				final ZoieSegmentReader<?> subreader = subreaders[i];
				for (int k=0;k<subuidarray.length;++k){ // k is the local DOC ID for the subreader
				  // subid is the global UID
					long subid = subuidarray[k]; // could be ZoieIndexReader.DELETED_UID
					if (subid != ZoieIndexReader.DELETED_UID)
					{
					  int localid = (int)(subid-_start); // this is local UID
					  if ((localid<0) || (localid>=uidArray.length))
					  {
					    log.error("Local UID outof range for localUID: " + localid);
					  }
					  uidArray[localid] = k+starts[i];//so the global DocID is this.
					}
				}
				subreader.setDocIDMapper(new DocIDMapper(){
				  int maxDoc = subreader.maxDoc();
				  int max = maxDoc + start;
				  
					public int getDocID(long uid) {
					  int mapped;
						uid = uid - _start;
						if (uid<0 || uid>=uidArray.length)
						{
						  return DocIDMapper.NOT_FOUND;
						} else
						{
						  mapped = uidArray[(int)uid];
						}
						if (mapped != DocIDMapper.NOT_FOUND){
						  if (mapped >= max)
						    return DocIDMapper.NOT_FOUND;
						  return mapped - start;
						}
						return mapped;
					}
				});
			}
			return new DocIDMapper() {
				public int getDocID(long uid) {
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
			};
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
			return new DocIDMapper() {
				// do the samething as DefaultDocIDMapperFactory since range does not really matter here
				public int getDocID(long uid) {
					for (int i = subreaders.length-1; i >= 0; --i){
						ZoieSegmentReader<?> subReader = subreaders[i];
						DocIDMapper mapper = subReader.getDocIDMaper();
						int docid = mapper.getDocID(uid);
						if (docid!=DocIDMapper.NOT_FOUND) {
							return docid+starts[i];
						}
					}
					return DocIDMapper.NOT_FOUND;
				}
			};
		}
		
	}
}
