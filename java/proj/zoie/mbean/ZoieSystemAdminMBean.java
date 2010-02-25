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

public interface ZoieSystemAdminMBean {
	int getDiskIndexSize();
	
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
	
	void refreshDiskReader()  throws IOException;
	
	Date getLastDiskIndexModifiedTime();
	  
	Date getLastOptimizationTime();
	  
	void optimize(int numSegs) throws IOException;
	
	void flushToDiskIndex() throws ZoieException;
	
	void purgeIndex() throws IOException;

	int getMaxBatchSize();
	  
	void setMaxBatchSize(int maxBatchSize);  
	
	void setMergeFactor(int mergeFactor);
	
	int getMergeFactor();
	
	void setMaxMergeDocs(int maxMergeDocs);
	
	int getMaxMergeDocs();
	
	void expungeDeletes() throws IOException;
	
	void setUseCompoundFile(boolean useCompoundFile);
	
	boolean isUseCompoundFile();
	
	int getDiskIndexSegmentCount() throws IOException;
	
	int getCurrentMemBatchSize();
    
    int getCurrentDiskBatchSize();
}
