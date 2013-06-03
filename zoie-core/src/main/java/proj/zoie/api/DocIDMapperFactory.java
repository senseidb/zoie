package proj.zoie.api;

import java.io.IOException;

import org.apache.lucene.index.AtomicReader;

public interface DocIDMapperFactory {
  DocIDMapper getDocIDMapper(AtomicReader reader) throws IOException;

  DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader) throws IOException;
}
