package proj.zoie.store;

import java.util.Comparator;

import proj.zoie.impl.indexing.AsyncDataConsumer;

public class AsyncZoieStoreConsumer<D> extends AsyncDataConsumer<D> {

  public AsyncZoieStoreConsumer(Comparator<String> versionComparator) {
    super(versionComparator);
  }
}
