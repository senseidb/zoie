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
  public final boolean isDeleted()
  {
    return false;
  }
}
