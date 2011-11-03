package proj.zoie.hourglass.impl;

import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieIndexReader;

public class NullDocIDMapper implements DocIDMapper<Object>
{
  public static final NullDocIDMapper INSTANCE = new NullDocIDMapper();
  public int getDocID(long uid)
  {
    return DocIDMapper.NOT_FOUND;
  }

  public Object getDocIDArray(long[] uids)
  {
    throw new UnsupportedOperationException();
  }

  public Object getDocIDArray(int[] uids)
  {
    throw new UnsupportedOperationException();
  }

  public int getReaderIndex(long uid)
  {
    throw new UnsupportedOperationException();
  }

  public int[] getStarts()
  {
    throw new UnsupportedOperationException();
  }

  public ZoieIndexReader<?>[] getSubReaders()
  {
    throw new UnsupportedOperationException();
  }

  public int quickGetDocID(long uid)
  {
    throw new UnsupportedOperationException();
  }  
}
