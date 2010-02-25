package proj.zoie.admin.client.jmx;

import java.io.Serializable;
import java.util.Date;

import com.google.gwt.user.client.rpc.IsSerializable;

public class ZoieServerInfo implements Serializable, IsSerializable
{

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private String[] names = null;
  public String[] getNames()
  {
    return names;
  }
  public void setNames(String[] names)
  {
    this.names = names;
  }
  public String[] getValues()
  {
    return values;
  }
  public void setValues(String[] values)
  {
    this.values = values;
  }
  private String[] values = null;
  private boolean[] readable = null;
  public boolean[] getReadable()
  {
    return readable;
  }
  public void setReadable(boolean[] readable)
  {
    this.readable = readable;
  }
  public boolean[] getWritable()
  {
    return writable;
  }
  public void setWritable(boolean[] writable)
  {
    this.writable = writable;
  }
  private boolean[] writable = null;
}
