package proj.zoie.mbean;

import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;

public interface ZoieAdminMBean {

    String getIndexDir();
    boolean isRealtime();
    long getBatchDelay();
    void setBatchDelay(long delay);
    int getBatchSize();
    void setBatchSize(int batchSize);

    Date getLastDiskIndexModifiedTime();


    int getMaxBatchSize();

    void setMaxBatchSize(int maxBatchSize);


    void setMergeFactor(int mergeFactor);

    int getMergeFactor();
    int getRamAIndexSize();

    String getRamAVersion();

    int getRamBIndexSize();

    String getRamBVersion();


    String getDiskIndexerStatus();
    String getCurrentDiskVersion() throws IOException;

    void refreshDiskReader() throws IOException;


    void flushToDiskIndex() throws ZoieException;

    void flushToMemoryIndex() throws ZoieException;

    int getMaxMergeDocs();  
    void setMaxMergeDocs(int maxMergeDocs);
    
    void setNumLargeSegments(int numLargeSegments);

    int getNumLargeSegments();

    void setMaxSmallSegments(int maxSmallSegments);

    public int getMaxSmallSegments();
    

    long getDiskIndexSizeBytes();

    long getDiskFreeSpaceBytes();

    boolean isUseCompoundFile();
    

    int getDiskIndexSegmentCount() throws IOException;

    int getRAMASegmentCount();

    int getRAMBSegmentCount();
    /**
     * @return the response time threshold for getIndexReaders
     */
    long getSLA();
    
    /**
     * @param sla set the response time threshold (expected max response time) for getIndexReaders
     */
    void setSLA(long sla);
    
    /**
     * @return heahth of the system. Non-zero value means the system need immediate attention and the logs need to be checked.
     */
    long getHealth();
    
    void resetHealth();
    

    int getCurrentMemBatchSize();

    int getCurrentDiskBatchSize();

}
