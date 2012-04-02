package proj.zoie.hourglass.impl;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;

public interface HourglassListener<R extends IndexReader, D> {
  public void onNewZoie(Zoie<R, D> zoie);
  public void onRetiredZoie(Zoie<R, D> zoie);
  public void onIndexReaderCleanUp(ZoieIndexReader<R> indexReader);
}
