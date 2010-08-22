package proj.zoie.hourglass.mbean;

import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;

public interface HourglassAdminMBean
{
  int getDiskIndexSize();

  long getDiskIndexSizeBytes();

  long getDiskFreeSpaceBytes();

  long getCurrentDiskVersion() throws IOException;

  int getRamAIndexSize();

  long getRamAVersion();

  int getRamBIndexSize();

  long getRamBVersion();

  String getDiskIndexerStatus();

  long getBatchDelay();

  void setBatchDelay(long delay);

  int getBatchSize();

  void setBatchSize(int batchSize);

  boolean isRealtime();

  String getIndexDir();

  void refreshDiskReader() throws IOException;

  Date getLastDiskIndexModifiedTime();

  void flushToDiskIndex() throws ZoieException;

  void flushToMemoryIndex() throws ZoieException;

  int getMaxBatchSize();

  void setMaxBatchSize(int maxBatchSize);

  int getMergeFactor();

  int getNumLargeSegments();

  public int getMaxSmallSegments();

  int getMaxMergeDocs();

  boolean isUseCompoundFile();

  int getDiskIndexSegmentCount() throws IOException;

  int getRAMASegmentCount();

  int getRAMBSegmentCount();

  int getCurrentMemBatchSize();

  int getCurrentDiskBatchSize();

  long getMinUID() throws IOException;

  long getMaxUID() throws IOException;
}
