package proj.zoie.api.impl;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;

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
