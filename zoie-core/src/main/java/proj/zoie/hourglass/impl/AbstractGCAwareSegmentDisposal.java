package proj.zoie.hourglass.impl;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.NumericDocValues;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;

public abstract class AbstractGCAwareSegmentDisposal implements
    HourglassListener<IndexReader, IndexReader> {

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
      ZoieSegmentReader[] segments = (ZoieSegmentReader[]) ((ZoieMultiReader) indexReader)
          .getSequentialSubReaders();
      for (ZoieSegmentReader segmentReader : segments) {
        handleSegment(segmentReader);
      }
    } else if (indexReader instanceof ZoieSegmentReader) {
      handleSegment((ZoieSegmentReader) indexReader);
    } else {
      throw new UnsupportedOperationException(
          "Only segment and multisegment readers can be handled");
    }

  }

  private void handleSegment(ZoieSegmentReader segmentReader) {
    onDelete(segmentReader, segmentReader.getUIDValues());
  }

  public abstract void onDelete(ZoieSegmentReader segmentReader, NumericDocValues uidValues);

  public abstract void shutdown();
}
