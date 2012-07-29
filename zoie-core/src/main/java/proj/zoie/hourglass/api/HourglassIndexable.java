/**
 * 
 */
package proj.zoie.hourglass.api;

import proj.zoie.api.indexing.AbstractZoieIndexable;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 *
 */
public abstract class HourglassIndexable extends AbstractZoieIndexable
{
  @Override
  public boolean isDeleted()
  {
    return false;
  }
}
