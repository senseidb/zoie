package proj.zoie.hourglass.impl;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;

public class NullDocIDMapperFactory implements DocIDMapperFactory
{
  public static final NullDocIDMapperFactory INSTANCE = new NullDocIDMapperFactory();

  @Override
  public DocIDMapper getDocIDMapper(AtomicReader reader) throws IOException {
    return NullDocIDMapper.INSTANCE;
  }
  @Override
  public DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader) throws IOException {
    return NullDocIDMapper.INSTANCE;
  }
}
