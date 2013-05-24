package proj.zoie.perf.client;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Random;

import org.json.JSONObject;

import proj.zoie.store.ZoieStore;

public class StoreQueryHandler implements QueryHandler<byte[]> {

  private final ZoieStore _store;
  private final int _numIds;
  private final long[] _idArray;
  private final Random _rand;

  public StoreQueryHandler(File dataFile, ZoieStore store, int numIds) throws Exception {
    _store = store;
    _numIds = numIds;

    _rand = new Random(System.currentTimeMillis());

    BufferedReader reader = null;

    LongArrayList idList = new LongArrayList(_numIds);
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile),
          Charset.forName("UTF-8")));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;
        try {
          JSONObject json = new JSONObject(line);
          long id = Long.parseLong(json.getString("id_str"));
          idList.add(id);
        } catch (Exception e) {
          // ignore
        }
        if (idList.size() >= _numIds) break;
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    _idArray = idList.toLongArray();
  }

  @Override
  public byte[] handleQuery() throws Exception {
    long uid = _idArray[_rand.nextInt(_idArray.length)];
    return _store.get(uid);
  }

  @Override
  public String getCurrentVersion() {
    return _store.getVersion();
  }

}
