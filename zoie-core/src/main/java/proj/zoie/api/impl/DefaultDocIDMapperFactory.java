package proj.zoie.api.impl;

import java.io.IOException;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;
import proj.zoie.api.indexing.AbstractZoieIndexable;

public class DefaultDocIDMapperFactory implements DocIDMapperFactory {

  @Override
  public DocIDMapper getDocIDMapper(final ZoieSegmentReader<?> reader) throws IOException {
    return new DocIDMapperImpl(
        reader.getNumericDocValues(AbstractZoieIndexable.DOCUMENT_ID_PAYLOAD_FIELD),
        reader.maxDoc(), reader.getLiveDocs());
  }

  @Override
  public DocIDMapper getDocIDMapper(final ZoieMultiReader<?> reader) throws IOException {
    final ZoieSegmentReader<?>[] zoieSegmentReaders = reader.getSubReaders();
    final DocIDMapper[] mappers = new DocIDMapper[zoieSegmentReaders.length];
    for (int i = 0; i < zoieSegmentReaders.length; ++i) {
      // reuse DocIDMapper since ZoieSegmentReader may be reused
      if (zoieSegmentReaders[i].getDocIDMapper() != null) {
        mappers[i] = zoieSegmentReaders[i].getDocIDMapper();
      } else {
        mappers[i] = getDocIDMapper(zoieSegmentReaders[i]);
        zoieSegmentReaders[i].setDocIDMapper(mappers[i]);
      }
    }

    return new DocIDMapper() {

      @Override
      public int getDocID(long uid) {
        int docid;
        for (int i = mappers.length - 1; i >= 0; --i) {
          docid = mappers[i].getDocID(uid);
          if (docid != DocIDMapper.NOT_FOUND) {
            return docid;
          }
        }
        return DocIDMapper.NOT_FOUND;
      }

      @Override
      public long[] getUIDArray() {
        throw new UnsupportedOperationException(
            "Only ZoieSegmentReader supports getUIDArray method");
      }
    };
  }
}
