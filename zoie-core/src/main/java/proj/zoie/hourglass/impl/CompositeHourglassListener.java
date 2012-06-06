package proj.zoie.hourglass.impl;

import java.util.List;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieIndexReader;

public class CompositeHourglassListener implements HourglassListener{
  private final List<HourglassListener> listeners;

  public CompositeHourglassListener(@SuppressWarnings("rawtypes") List<HourglassListener> listeners) {
    this.listeners = listeners;     
  }

  @Override
  public void onNewZoie(Zoie zoie) {
    for (HourglassListener listener : listeners) {
      if (zoie != null) listener.onNewZoie(zoie);
    }    
  }

  @Override
  public void onRetiredZoie(Zoie zoie) {
    for (HourglassListener listener : listeners) {
      if (zoie != null) listener.onRetiredZoie(zoie);
    }
  }

  @Override
  public void onIndexReaderCleanUp(ZoieIndexReader indexReader) {
    for (HourglassListener listener : listeners) {
      if (indexReader != null) listener.onIndexReaderCleanUp(indexReader);
    }
  }
 

}
