package proj.zoie.impl.indexing.internal;

import java.io.IOException;

import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

public class ZoieSegmentTermPositions extends ZoieSegmentTermDocs implements TermPositions
{
	  final TermPositions _tp;
	  
	  public ZoieSegmentTermPositions(TermPositions in, DocIdSetIterator delSetIterator) throws IOException
	  {
	    super(in, delSetIterator);
	    _tp = in;
	  }
	  
	  public ZoieSegmentTermPositions(TermPositions in, DocIdSet delSet) throws IOException
	  {
	    this(in,delSet.iterator());
	  }
	  
	  public int nextPosition() throws IOException
	  {
	    return _tp.nextPosition();
	  }

	  public int getPayloadLength()
	  {
	    return _tp.getPayloadLength();
	  }

	  public byte[] getPayload(byte[] data, int offset) throws IOException
	  {
	    return _tp.getPayload(data, offset);
	  }
	  
	  public boolean isPayloadAvailable()
	  {
	    return _tp.isPayloadAvailable();
	  }
	}