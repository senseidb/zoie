/**
 * 
 */
package proj.zoie.hourglass.mbean;

import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;
import proj.zoie.hourglass.impl.Hourglass;

/**
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 * 
 */
public class HourglassAdmin implements HourglassAdminMBean
{

  private final Hourglass<?, ?, ?> hourglass;
  public HourglassAdmin(Hourglass<?, ?, ?> hourglass)
  {
    this.hourglass = hourglass;
  }


    @Override
    public void flushToDiskIndex() throws ZoieException
    {
    }

    @Override
    public void flushToMemoryIndex() throws ZoieException
    {
      // TODO Auto-generated method stub

    }

    @Override
    public long getBatchDelay()
    {
      return hourglass.getzConfig().getBatchDelay();
    }

    @Override
    public int getBatchSize()
    {
      return hourglass.getzConfig().getBatchSize();
    }

    @Override
    public int getCurrentDiskBatchSize()
    {
      return hourglass.getCurrentZoie().getCurrentDiskBatchSize();
    }

    @Override
    public String getCurrentDiskVersion() throws IOException
    {
      return hourglass.getCurrentZoie().getCurrentDiskVersion();
    }

    @Override
    public int getCurrentMemBatchSize()
    {
      return hourglass.getCurrentZoie().getCurrentMemBatchSize();
    }

    @Override
    public long getDiskFreeSpaceBytes()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getDiskFreeSpaceBytes();
    }

    @Override
    public int getDiskIndexSegmentCount() throws IOException
    {
      return hourglass.getCurrentZoie().getAdminMBean().getDiskIndexSegmentCount();
    }

    @Override
    public int getDiskIndexSize()
    {
      return 0;
    }

    @Override
    public long getDiskIndexSizeBytes()
    {
      return hourglass.getSizeBytes();
    }

    @Override
    public String getDiskIndexerStatus()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getDiskIndexerStatus();
    }

    @Override
    public String getIndexDir()
    {
      return hourglass.getDirMgrFactory().getRoot().getAbsolutePath();
    }

    @Override
    public Date getLastDiskIndexModifiedTime()
    {
      return hourglass.getCurrentZoie().getAdminMBean()
          .getLastDiskIndexModifiedTime();
    }

    @Override
    public int getMaxBatchSize()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getMaxBatchSize();
    }

    @Override
    public int getMaxMergeDocs()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getMaxMergeDocs();
    }

    @Override
    public int getMaxSmallSegments()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getMaxSmallSegments();
    }

    @Override
    public long getMaxUID() throws IOException
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getMergeFactor()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getMergeFactor();
    }

    @Override
    public long getMinUID() throws IOException
    {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getNumLargeSegments()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getNumLargeSegments();
    }

    @Override
    public int getRAMASegmentCount()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRAMASegmentCount();
    }

    @Override
    public int getRAMBSegmentCount()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRAMBSegmentCount();
    }

    @Override
    public int getRamAIndexSize()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRamAIndexSize();
    }

    @Override
    public String getRamAVersion()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRamAVersion();
    }

    @Override
    public int getRamBIndexSize()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRamBIndexSize();
    }

    @Override
    public String getRamBVersion()
    {
      return hourglass.getCurrentZoie().getAdminMBean().getRamBVersion();
    }

    @Override
    public boolean isRealtime()
    {
      return hourglass.getCurrentZoie().getAdminMBean().isRealtime();
    }

    @Override
    public boolean isUseCompoundFile()
    {
      return hourglass.getCurrentZoie().getAdminMBean().isUseCompoundFile();
    }

    @Override
    public void refreshDiskReader() throws IOException
    {
      hourglass.getCurrentZoie().getAdminMBean().refreshDiskReader();
    }

    @Override
    public void setBatchDelay(long delay)
    {
      hourglass.getzConfig().setBatchDelay(delay);
      hourglass.getCurrentZoie().getAdminMBean().setBatchDelay(delay);
    }

    @Override
    public void setBatchSize(int batchSize)
    {
      hourglass.getzConfig().setBatchSize(batchSize);
      hourglass.getCurrentZoie().getAdminMBean().setBatchSize(batchSize);
    }

    @Override
    public void setMaxBatchSize(int maxBatchSize)
    {
      hourglass.getzConfig().setMaxBatchSize(maxBatchSize);
      hourglass.getCurrentZoie().getAdminMBean().setMaxBatchSize(maxBatchSize);
    }
}
