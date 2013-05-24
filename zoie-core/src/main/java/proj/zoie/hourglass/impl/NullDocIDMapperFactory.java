package proj.zoie.hourglass.impl;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.ZoieMultiReader;

public class NullDocIDMapperFactory implements DocIDMapperFactory {
  public static final NullDocIDMapperFactory INSTANCE = new NullDocIDMapperFactory();

  public DocIDMapper<Object> getDocIDMapper(ZoieMultiReader<?> reader) {
    for (ZoieIndexReader<?> r : reader.getSequentialSubReaders()) {
      r.setDocIDMapper(NullDocIDMapper.INSTANCE);
    }
    return NullDocIDMapper.INSTANCE;
  }
}
