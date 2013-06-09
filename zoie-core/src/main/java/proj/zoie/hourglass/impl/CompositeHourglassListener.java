package proj.zoie.hourglass.impl;

import java.util.List;

import org.apache.lucene.index.IndexReader;

import proj.zoie.api.Zoie;
import proj.zoie.api.ZoieMultiReader;

public class CompositeHourglassListener<R extends IndexReader, D> implements HourglassListener<R, D> {
  private final List<HourglassListener<R,D>> listeners;

  public CompositeHourglassListener(List<HourglassListener<R, D>> hourglassListeners) {
    this.listeners = hourglassListeners;
  }

  @Override
  public void onNewZoie(Zoie<R, D> zoie) {
    for (HourglassListener<R, D> listener : listeners) {
      if (zoie != null) listener.onNewZoie(zoie);
    }
  }

  @Override
  public void onRetiredZoie(Zoie<R, D> zoie) {
    for (HourglassListener<R, D> listener : listeners) {
      if (zoie != null) listener.onRetiredZoie(zoie);
    }
  }

  @Override
  public void onIndexReaderCleanUp(ZoieMultiReader<R> indexReader) {
    for (HourglassListener<R, D> listener : listeners) {
      if (indexReader != null) listener.onIndexReaderCleanUp(indexReader);
    }
  }

}
