package proj.zoie.api.impl;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.index.AtomicReaderContext;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.DocIDMapperFactory;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.ZoieSegmentReader;

public class DefaultDocIDMapperFactory implements DocIDMapperFactory {

  @Override
  public DocIDMapper getDocIDMapper(final ZoieSegmentReader<?> reader) throws IOException {
    return new DocIDMapperImpl(ZoieReaderUtil.getUidValues(reader), reader.maxDoc());
  }

  @Override
  public DocIDMapper getDocIDMapper(final ZoieMultiReader<?> reader) throws IOException{
    final List<AtomicReaderContext> subReaderContextList = reader.getInnerReader().leaves();
    final DocIDMapper[] mappers = new DocIDMapper[subReaderContextList.size()];
    for (int i = 0; i < subReaderContextList.size(); ++i) {
      mappers[i] = getDocIDMapper(subReaderContextList.get(i).reader());
    }

    return new DocIDMapper() {

      @Override
      public int quickGetDocID(long uid) {
        int docid;
        for (int i = mappers.length-1; i >= 0; --i) {
          docid = mappers[i].getDocID(uid);
          if (docid != DocIDMapper.NOT_FOUND) {
            return docid;
          }
        }
        return DocIDMapper.NOT_FOUND;
      }

      @Override
      public int getDocID(long uid) {
        return quickGetDocID(uid);
      }
    };
  }
}
