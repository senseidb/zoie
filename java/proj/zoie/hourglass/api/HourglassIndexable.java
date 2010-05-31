/**
 * 
 */
package proj.zoie.hourglass.api;

import proj.zoie.api.indexing.ZoieIndexable;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public abstract class HourglassIndexable implements ZoieIndexable
{
  protected static long nextUID = System.currentTimeMillis();
  public final long UID;
  public HourglassIndexable()
  {
    UID = getNextUID();
  }
  public static final synchronized long getNextUID()
  {
    return nextUID++;
  }
  public final long getUID()
  {
    return UID;
  }
  public final boolean isDeleted()
  {
    return false;
  }
}
