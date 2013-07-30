package proj.zoie.api;

import java.io.IOException;

public interface DocIDMapperFactory {
  DocIDMapper getDocIDMapper(ZoieSegmentReader<?> reader) throws IOException;

  DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader) throws IOException;
}
