package proj.zoie.impl.indexing.internal;

import java.io.IOException;

import org.apache.lucene.index.FilterIndexReader.FilterTermDocs;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class ZoieSegmentTermDocs extends FilterTermDocs {
	private final DocIdSet delSet;
	private int _firstDelDoc = -1;
	private DocIdSetIterator _delSetIterator;
    private int _nextDelDoc;
    
	public ZoieSegmentTermDocs(TermDocs in,DocIdSet delSet) throws IOException{
		super(in);
		this.delSet = delSet;
		resetDelIter();
		_firstDelDoc = _nextDelDoc;
	}
	
	private final void resetDelIter() throws IOException {
		if (_firstDelDoc != _nextDelDoc) {
			_delSetIterator = delSet.iterator();
			_nextDelDoc = _delSetIterator.nextDoc();
		}
	}
	
	@Override
	public void seek(Term term) throws IOException {
		resetDelIter();
		super.seek(term);
	}

	@Override
	public void seek(TermEnum termEnum) throws IOException {
		resetDelIter();
		super.seek(termEnum);
	}

	public boolean next() throws IOException {
		boolean hasNext=in.next();
		if (_nextDelDoc!=DocIdSetIterator.NO_MORE_DOCS){
          int currID =in.doc();
			while(hasNext){
				if (currID<_nextDelDoc){
					return hasNext;
				}
				else{
					if (currID == _nextDelDoc){
						hasNext=in.next();
			            currID =in.doc();
					}
					_nextDelDoc = _delSetIterator.advance(currID);
				}
			}
		}
		return hasNext;
	}

	public int read(final int[] docs, final int[] freqs) throws IOException {
		if (_nextDelDoc!=DocIdSetIterator.NO_MORE_DOCS){
			int i = 0;
			while (i < docs.length) {
				if (!in.next())
					return i;
	
				int doc = in.doc();
				if (doc<_nextDelDoc){
					docs[i] = doc;
					freqs[i] = in.freq();
					i++;
				}
				else{
				  _nextDelDoc = _delSetIterator.advance(doc);
					if (doc==_nextDelDoc){
						continue;
					}
					else{
						docs[i] = doc;
						freqs[i] = in.freq();
						i++;
					}
				}
			}
			return i;
		}
		else{
		  return in.read(docs,freqs);
		}
	}

	public boolean skipTo(int i) throws IOException {
		if (!in.skipTo(i))
			return false;

		if (_nextDelDoc!=DocIdSetIterator.NO_MORE_DOCS){
		  int doc = in.doc();
		  if (doc<_nextDelDoc) return true;
		  _nextDelDoc = _delSetIterator.advance(doc);
		  if (doc==_nextDelDoc) return next();
		}
		return true;
	}
}
