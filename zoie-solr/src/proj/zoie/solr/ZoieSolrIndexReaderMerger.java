package proj.zoie.solr;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.IndexReaderMerger;
import proj.zoie.api.ZoieIndexReader;

public class ZoieSolrIndexReaderMerger<T extends IndexReader> implements IndexReaderMerger<MultiReader, T> {
	public MultiReader mergeIndexReaders(List<ZoieIndexReader<T>> readerList) {
		MultiReader r = new ZoieSolrMultiReader(readerList.toArray(new ZoieIndexReader[readerList.size()]));
		return r;
	}
	
	private static class ZoieSolrMultiReader extends MultiReader{

		private ZoieIndexReader _diskReader = null;
		public ZoieSolrMultiReader(ZoieIndexReader[] subReaders) {
			super(subReaders,false);
			for (ZoieIndexReader subReader : subReaders){
				Directory dir = subReader.directory();
				if (dir instanceof FSDirectory){
					_diskReader = subReader;
					break;
				}
			}
		}
		@Override
		public Directory directory() {
			return _diskReader.directory();
		}
		@Override
		public long getVersion() {
			return _diskReader.getVersion();
		}	
		@Override
		public IndexCommit getIndexCommit() throws IOException {
			Directory dir = _diskReader.directory();
			SegmentInfos segmentInfos = new SegmentInfos();
			segmentInfos.read(dir);
			return new ZoieSolrIndexCommit(segmentInfos, dir);
		}
		
	}
}
