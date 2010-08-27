package proj.zoie.mbean;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.IOException;
import java.util.Date;

import proj.zoie.api.ZoieException;
import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieSystemAdmin implements ZoieSystemAdminMBean {
	private final ZoieSystemAdminMBean _internalMBean;
	
	@SuppressWarnings("unchecked")
	public ZoieSystemAdmin(ZoieSystem zoieSystem)
	{
		_internalMBean=zoieSystem.getAdminMBean();
	}
	
	public void refreshDiskReader()  throws IOException{
		_internalMBean.refreshDiskReader();
	}

	public long getBatchDelay() {
		return _internalMBean.getBatchDelay();
	}

	public int getBatchSize() {
		return _internalMBean.getBatchSize();
	}

	public long getCurrentDiskVersion() throws IOException{
		return _internalMBean.getCurrentDiskVersion();
	}

	public int getDiskIndexSize() {
		return _internalMBean.getDiskIndexSize();
	}

 	public long getDiskIndexSizeBytes()
 	{
 	  return _internalMBean.getDiskIndexSizeBytes();
 	}

	@Override
  public long getDiskFreeSpaceBytes()
  {
    return _internalMBean.getDiskFreeSpaceBytes();
  }

	public String getDiskIndexerStatus() {
		return _internalMBean.getDiskIndexerStatus();
	}

	public Date getLastDiskIndexModifiedTime() {
		return _internalMBean.getLastDiskIndexModifiedTime();
	}

	public Date getLastOptimizationTime() {
		return _internalMBean.getLastOptimizationTime();
	}

	public int getMaxBatchSize() {
		return _internalMBean.getMaxBatchSize();
	}
	
	public int getRamAIndexSize() {
		return _internalMBean.getRamAIndexSize();
	}

	public long getRamAVersion() {
		return _internalMBean.getRamAVersion();
	}

	public int getRamBIndexSize() {
		return _internalMBean.getRamBIndexSize();
	}

	public long getRamBVersion() {
		return _internalMBean.getRamBVersion();
	}

	public void optimize(int numSegs) throws IOException {
		_internalMBean.optimize(numSegs);
	}

	public void setBatchSize(int batchSize) {
		_internalMBean.setBatchSize(batchSize);
	}

	public void setMaxBatchSize(int maxBatchSize) {
		_internalMBean.setMaxBatchSize(maxBatchSize);
	}

	public String getIndexDir() {
		return _internalMBean.getIndexDir();
	}

	public boolean isRealtime() {
		return _internalMBean.isRealtime();
	}

	public void setBatchDelay(long delay) {
		_internalMBean.setBatchDelay(delay);
	}

	public void flushToDiskIndex() throws ZoieException{
		_internalMBean.flushToDiskIndex();
	}
	
    public void flushToMemoryIndex() throws ZoieException
    {
      _internalMBean.flushToMemoryIndex();
      
    }

	public void purgeIndex() throws IOException
	{
		_internalMBean.purgeIndex();
	}

	public void expungeDeletes() throws IOException {
		_internalMBean.expungeDeletes();
	}

	public int getMaxMergeDocs() {
		return _internalMBean.getMaxBatchSize();
	}

	public int getMergeFactor() {
		return _internalMBean.getMergeFactor();
	}

	public void setMaxMergeDocs(int maxMergeDocs) {
		_internalMBean.setMaxMergeDocs(maxMergeDocs);
	}

	public void setMergeFactor(int mergeFactor) {
		_internalMBean.setMergeFactor(mergeFactor);
	}

	public boolean isUseCompoundFile() {
		return _internalMBean.isUseCompoundFile();
	}

	public void setUseCompoundFile(boolean useCompoundFile) {
		_internalMBean.setUseCompoundFile(useCompoundFile);
	}
    
    public int getCurrentMemBatchSize()
    {
      return _internalMBean.getCurrentMemBatchSize(); 
    }
    
    public int getCurrentDiskBatchSize()
    {
      return _internalMBean.getCurrentDiskBatchSize(); 
    }
    
    public int getDiskIndexSegmentCount() throws IOException{
		return _internalMBean.getDiskIndexSegmentCount();
	}
    public int getRAMASegmentCount()
    {
      return _internalMBean.getRAMASegmentCount();
    }
    public int getRAMBSegmentCount()
    {
      return _internalMBean.getRAMBSegmentCount();
    }

    public int getMaxSmallSegments()
    {
      return _internalMBean.getMaxSmallSegments();
    }
    
    public void setMaxSmallSegments(int maxSmallSegments)
    {
      _internalMBean.setMaxSmallSegments(maxSmallSegments);
    }
    
    public int getNumLargeSegments()
    {
      return _internalMBean.getNumLargeSegments();
    }
    
    public void setNumLargeSegments(int numLargeSegments)
    {
      _internalMBean.setNumLargeSegments(numLargeSegments);
    }
    
    public long getMinUID() throws IOException
    {
      return _internalMBean.getMinUID();
    }
    
    public long getMaxUID() throws IOException
    {
      return _internalMBean.getMaxUID();
    }

    @Override
    public long getHealth()
    {
      return _internalMBean.getHealth();
    }

    @Override
    public void resetHealth()
    {
      _internalMBean.resetHealth();
    }

    @Override
    public long getSLA()
    {
      return _internalMBean.getSLA();
    }

    @Override
    public void setSLA(long sla)
    {
      _internalMBean.setSLA(sla);
    }
}
