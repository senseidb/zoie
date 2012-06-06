package proj.zoie.hourglass.impl;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.hourglass.impl.HourGlassScheduler.FREQUENCY;

public abstract class AbstractGCAwareSegmentDisposal implements HourglassListener<IndexReader, IndexReader>{
  
  @Override
  public void onNewZoie(Zoie<IndexReader, IndexReader> zoie) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onRetiredZoie(Zoie<IndexReader, IndexReader> zoie) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onIndexReaderCleanUp(ZoieIndexReader<IndexReader> indexReader) {
    if (indexReader instanceof ZoieMultiReader) {
      ZoieSegmentReader[] segments = (ZoieSegmentReader[]) ((ZoieMultiReader) indexReader).getSequentialSubReaders();
      for (ZoieSegmentReader segmentReader : segments) {
        handleSegment(segmentReader);
      }
    } else if (indexReader instanceof ZoieSegmentReader) {
      handleSegment((ZoieSegmentReader) indexReader);
    } else {
      throw new UnsupportedOperationException("Only segment and multisegment readers can be handled");
    }
    
  }
  private void handleSegment(ZoieSegmentReader segmentReader) {    
    onDelete(segmentReader, segmentReader.getUIDArray());      
  }

  public abstract void onDelete(ZoieSegmentReader segmentReader, long[] uidArray) ;
  public abstract void shutdown();
}
