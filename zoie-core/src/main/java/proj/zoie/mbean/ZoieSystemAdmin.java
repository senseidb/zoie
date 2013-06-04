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

  @SuppressWarnings("rawtypes")
  public ZoieSystemAdmin(ZoieSystem zoieSystem) {
    _internalMBean = zoieSystem.getAdminMBean();
  }

  @Override
  public void refreshDiskReader() throws IOException {
    _internalMBean.refreshDiskReader();
  }

  @Override
  public long getBatchDelay() {
    return _internalMBean.getBatchDelay();
  }

  @Override
  public int getBatchSize() {
    return _internalMBean.getBatchSize();
  }

  @Override
  public String getCurrentDiskVersion() throws IOException {
    return _internalMBean.getCurrentDiskVersion();
  }

  @Override
  public int getDiskIndexSize() {
    return _internalMBean.getDiskIndexSize();
  }

  @Override
  public long getDiskIndexSizeBytes() {
    return _internalMBean.getDiskIndexSizeBytes();
  }

  @Override
  public long getDiskFreeSpaceBytes() {
    return _internalMBean.getDiskFreeSpaceBytes();
  }

  @Override
  public String getDiskIndexerStatus() {
    return _internalMBean.getDiskIndexerStatus();
  }

  @Override
  public Date getLastDiskIndexModifiedTime() {
    return _internalMBean.getLastDiskIndexModifiedTime();
  }

  @Override
  public int getMaxBatchSize() {
    return _internalMBean.getMaxBatchSize();
  }

  @Override
  public int getRamAIndexSize() {
    return _internalMBean.getRamAIndexSize();
  }

  @Override
  public String getRamAVersion() {
    return _internalMBean.getRamAVersion();
  }

  @Override
  public int getRamBIndexSize() {
    return _internalMBean.getRamBIndexSize();
  }

  @Override
  public String getRamBVersion() {
    return _internalMBean.getRamBVersion();
  }

  @Override
  public void optimize(int numSegs) throws IOException {
    _internalMBean.optimize(numSegs);
  }

  @Override
  public void setBatchSize(int batchSize) {
    _internalMBean.setBatchSize(batchSize);
  }

  @Override
  public void setMaxBatchSize(int maxBatchSize) {
    _internalMBean.setMaxBatchSize(maxBatchSize);
  }

  @Override
  public String getIndexDir() {
    return _internalMBean.getIndexDir();
  }

  @Override
  public boolean isRealtime() {
    return _internalMBean.isRealtime();
  }

  @Override
  public void setBatchDelay(long delay) {
    _internalMBean.setBatchDelay(delay);
  }

  @Override
  public void flushToDiskIndex() throws ZoieException {
    _internalMBean.flushToDiskIndex();
  }

  @Override
  public void flushToMemoryIndex() throws ZoieException {
    _internalMBean.flushToMemoryIndex();

  }

  @Override
  public void purgeIndex() throws IOException {
    _internalMBean.purgeIndex();
  }

  @Override
  public void expungeDeletes() throws IOException {
    _internalMBean.expungeDeletes();
  }

  @Override
  public int getMaxMergeDocs() {
    return _internalMBean.getMaxBatchSize();
  }

  @Override
  public int getMergeFactor() {
    return _internalMBean.getMergeFactor();
  }

  @Override
  public void setMaxMergeDocs(int maxMergeDocs) {
    _internalMBean.setMaxMergeDocs(maxMergeDocs);
  }

  @Override
  public void setMergeFactor(int mergeFactor) {
    _internalMBean.setMergeFactor(mergeFactor);
  }

  @Override
  public boolean isUseCompoundFile() {
    return _internalMBean.isUseCompoundFile();
  }

  @Override
  public void setUseCompoundFile(boolean useCompoundFile) {
    _internalMBean.setUseCompoundFile(useCompoundFile);
  }

  @Override
  public int getCurrentMemBatchSize() {
    return _internalMBean.getCurrentMemBatchSize();
  }

  @Override
  public int getCurrentDiskBatchSize() {
    return _internalMBean.getCurrentDiskBatchSize();
  }

  @Override
  public int getDiskIndexSegmentCount() throws IOException {
    return _internalMBean.getDiskIndexSegmentCount();
  }

  @Override
  public int getRAMASegmentCount() {
    return _internalMBean.getRAMASegmentCount();
  }

  @Override
  public int getRAMBSegmentCount() {
    return _internalMBean.getRAMBSegmentCount();
  }

  @Override
  public int getMaxSmallSegments() {
    return _internalMBean.getMaxSmallSegments();
  }

  @Override
  public void setMaxSmallSegments(int maxSmallSegments) {
    _internalMBean.setMaxSmallSegments(maxSmallSegments);
  }

  @Override
  public int getNumLargeSegments() {
    return _internalMBean.getNumLargeSegments();
  }

  @Override
  public void setNumLargeSegments(int numLargeSegments) {
    _internalMBean.setNumLargeSegments(numLargeSegments);
  }

  @Override
  public long getHealth() {
    return _internalMBean.getHealth();
  }

  @Override
  public void resetHealth() {
    _internalMBean.resetHealth();
  }

  @Override
  public long getSLA() {
    return _internalMBean.getSLA();
  }

  @Override
  public void setSLA(long sla) {
    _internalMBean.setSLA(sla);
  }

  @Override
  public long getFreshness() {
    return _internalMBean.getFreshness();
  }

  @Override
  public void setFreshness(long freshness) {
    _internalMBean.setFreshness(freshness);
  }
}
