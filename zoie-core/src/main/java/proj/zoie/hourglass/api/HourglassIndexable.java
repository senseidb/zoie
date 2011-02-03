/**
 * 
 */
package proj.zoie.hourglass.api;

import java.io.Serializable;

import proj.zoie.api.indexing.ZoieIndexable;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public abstract class HourglassIndexable<VALUE extends Serializable> implements ZoieIndexable<VALUE>
{
  public final boolean isDeleted()
  {
    return false;
  }
}
