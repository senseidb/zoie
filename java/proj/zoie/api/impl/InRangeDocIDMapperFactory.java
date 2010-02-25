package proj.zoie.api.impl;

import java.util.Arrays;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;

public class InRangeDocIDMapperFactory implements DocIDMapperFactory {
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
			final int[] uidArray = new int[_count];
			Arrays.fill(uidArray,DocIDMapper.NOT_FOUND);
			for (int i = 0; i < subreaders.length; ++i){
				long[] subuidarray = subreaders[i].getUIDArray();
				final int start = starts[i];
				final ZoieSegmentReader<?> subreader = subreaders[i];
				for (int k=0;k<subuidarray.length;++k){
					long subid = subuidarray[k];
					uidArray[(int)(subid-_start)] = k+starts[i];
				}
				subreader.setDocIDMapper(new DocIDMapper(){
					public int getDocID(long uid) {
						uid = uid - _start;
						int mapped = uidArray[(int)uid];
						if (mapped != DocIDMapper.NOT_FOUND){
							mapped -= start;
							if (mapped >= subreader.maxDoc()){
								mapped = DocIDMapper.NOT_FOUND;
							}
						}
						return mapped;
					}
				});
			}
			return new DocIDMapper() {
				public int getDocID(long uid) {
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
