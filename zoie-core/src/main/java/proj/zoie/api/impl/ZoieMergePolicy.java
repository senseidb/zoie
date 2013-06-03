package proj.zoie.api.impl;

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
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;

/**
 * @author ymatsuda
 *
 */
public class ZoieMergePolicy extends LogByteSizeMergePolicy {
  public static final Logger log = Logger.getLogger(ZoieMergePolicy.class.getName());
  public static final int DEFAULT_NUM_LARGE_SEGMENTS = 6;
  public static final int DEFAULT_NUM_SMALL_SEGMENTS = 7;
  public static final int DEFAULT_MERGE_FACTOR = 6;

  private boolean _partialExpunge = false;
  private int _numLargeSegments = DEFAULT_NUM_LARGE_SEGMENTS;
  private int _maxSmallSegments = DEFAULT_NUM_SMALL_SEGMENTS; // default merge factor plus 1.
  private int _maxSegments = _numLargeSegments + _maxSmallSegments;

  public ZoieMergePolicy() {
    super.setMergeFactor(DEFAULT_MERGE_FACTOR);// set default merge factor to 7. Less than 10. Good
                                               // for search speed.
  }

  public void setMergePolicyParams(MergePolicyParams params) {
    if (params != null) {
      setPartialExpunge(params._doPartialExpunge);
      setNumLargeSegments(params._numLargeSegments);
      setMergeFactor(params._mergeFactor);
      setMaxSmallSegments(params._maxSmallSegments);
      setUseCompoundFile(params._useCompoundFile);
      setMaxMergeDocs(params._maxMergeDocs);
    }
  }

  protected long size(SegmentInfo info) throws IOException {
    long byteSize = info.sizeInBytes(false);
    float delRatio = (info.docCount <= 0 ? 0.0f
        : ((float) info.getDelCount() / (float) info.docCount));
    return (info.docCount <= 0 ? byteSize : (long) (byteSize * (1.0f - delRatio)));
  }

  public void setPartialExpunge(boolean doPartialExpunge) {
    _partialExpunge = doPartialExpunge;
  }

  public boolean getPartialExpunge() {
    return _partialExpunge;
  }

  public void setNumLargeSegments(int numLargeSegments) {
    if (numLargeSegments < 2) {
      log.warn("numLargeSegments cannot be less than 2, while " + numLargeSegments
          + " is requested. Override with 2.");
      numLargeSegments = 2;
    }
    _numLargeSegments = numLargeSegments;
    _maxSegments = _numLargeSegments + 2 * getMergeFactor();
  }

  public int getNumLargeSegments() {
    return _numLargeSegments;
  }

  public void setMaxSmallSegments(int maxSmallSegments) {
    if (maxSmallSegments < getMergeFactor() + 1) {
      log.warn("MergeFactor is "
          + getMergeFactor()
          + ". maxSmallSegments is requested to be: "
          + maxSmallSegments
          + ". Override with mergeFactor + 1, since maxSmallSegments has to be greater than mergeFactor.");
      maxSmallSegments = getMergeFactor() + 1;
    }
    _maxSmallSegments = maxSmallSegments;
    _maxSegments = _numLargeSegments + _maxSmallSegments;
  }

  public int getMaxSmallSegments() {
    return _maxSmallSegments;
  }

  @Override
  public void setMergeFactor(int mergeFactor) {
    if (mergeFactor < 2) {
      log.warn("mergeFactor has to be at least 2. Override " + mergeFactor + " with 2");
      mergeFactor = 2;
    }
    super.setMergeFactor(mergeFactor);
    if (_maxSmallSegments < getMergeFactor()) {
      log.warn("maxSmallSegments has to be greater than mergeFactor. Override maxSmallSegments to: "
          + (mergeFactor + 1));
      _maxSmallSegments = getMergeFactor() + 1;
      _maxSegments = _numLargeSegments + _maxSmallSegments;
    }
  }

  @Override
  protected boolean isMerged(SegmentInfos infos, int maxNumSegments,
      Map<SegmentInfo, Boolean> segmentsToOptimize) throws IOException {
    final int numSegments = infos.size();
    int numToOptimize = 0;
    SegmentInfo optimizeInfo = null;
    for (int i = 0; i < numSegments && numToOptimize <= maxNumSegments; i++) {
      final SegmentInfo info = infos.info(i);
      if (segmentsToOptimize.get(info)) {
        numToOptimize++;
        optimizeInfo = info;
      }
    }

    return numToOptimize <= maxNumSegments && (numToOptimize != 1 || isMerged(optimizeInfo));
  }

  /** Returns true if this single nfo is optimized (has no
   *  pending norms or deletes, is in the same dir as the
   *  writer, and matches the current compound file setting */
  @Override
  protected boolean isMerged(SegmentInfo info) throws IOException {
    IndexWriter w = writer.get();
    return !info.hasDeletions() && !info.hasSeparateNorms() && info.dir == w.getDirectory()
        && info.getUseCompoundFile() == getUseCompoundFile();
  }

  /** Returns the merges necessary to optimize the index.
   *  This merge policy defines "optimized" to mean only one
   *  segment in the index, where that segment has no
   *  deletions pending nor separate norms, and it is in
   *  compound file format if the current useCompoundFile
   *  setting is true.  This method returns multiple merges
   *  (mergeFactor at a time) so the {@link MergeScheduler}
   *  in use may make use of concurrency. */
  @Override
  public MergeSpecification findForcedMerges(SegmentInfos infos, int maxNumSegments,
      Map<SegmentInfo, Boolean> segmentsToOptimize) throws IOException {

    assert maxNumSegments > 0;

    MergeSpecification spec = null;

    if (!isMerged(infos, maxNumSegments, segmentsToOptimize)) {
      // Find the newest (rightmost) segment that needs to
      // be optimized (other segments may have been flushed
      // since optimize started):
      int last = infos.size();
      while (last > 0) {
        final SegmentInfo info = infos.info(--last);
        if (segmentsToOptimize.get(info)) {
          last++;
          break;
        }
      }

      if (last > 0) {
        if (maxNumSegments == 1) {
          // Since we must optimize down to 1 segment, the
          // choice is simple:
          // boolean useCompoundFile = getUseCompoundFile();
          if (last > 1 || !isMerged(infos.info(0))) {
            spec = new MergeSpecification();
            spec.add(new OneMerge(infos.asList().subList(0, last)));
          }
        } else if (last > maxNumSegments) {
          // find most balanced merges
          spec = findBalancedMerges(infos, last, maxNumSegments, _partialExpunge);
        }
      }
    }
    return spec;
  }

  private MergeSpecification findBalancedMerges(SegmentInfos infos, int infoLen,
      int maxNumSegments, boolean partialExpunge) throws IOException {
    if (infoLen <= maxNumSegments) return null;

    MergeSpecification spec = new MergeSpecification();
    // boolean useCompoundFile = getUseCompoundFile();

    // use Viterbi algorithm to find the best segmentation.
    // we will try to minimize the size variance of resulting segments.

    double[][] variance = createVarianceTable(infos, infoLen, maxNumSegments);

    final int maxMergeSegments = infoLen - maxNumSegments + 1;
    double[] sumVariance = new double[maxMergeSegments];
    int[][] backLink = new int[maxNumSegments][maxMergeSegments];

    for (int i = (maxMergeSegments - 1); i >= 0; i--) {
      sumVariance[i] = variance[0][i];
      backLink[0][i] = 0;
    }
    for (int i = 1; i < maxNumSegments; i++) {
      for (int j = (maxMergeSegments - 1); j >= 0; j--) {
        double minV = Double.MAX_VALUE;
        int minK = 0;
        for (int k = j; k >= 0; k--) {
          double v = sumVariance[k] + variance[i + k][j - k];
          if (v < minV) {
            minV = v;
            minK = k;
          }
        }
        sumVariance[j] = minV;
        backLink[i][j] = minK;
      }
    }

    // now, trace back the back links to find all merges,
    // also find a candidate for partial expunge if requested
    int mergeEnd = infoLen;
    int prev = maxMergeSegments - 1;
    int expungeCandidate = -1;
    int maxDelCount = 0;
    for (int i = maxNumSegments - 1; i >= 0; i--) {
      prev = backLink[i][prev];
      int mergeStart = i + prev;
      if ((mergeEnd - mergeStart) > 1) {
        spec.add(new OneMerge(infos.asList().subList(mergeStart, mergeEnd)));
      } else {
        if (partialExpunge) {
          SegmentInfo info = infos.info(mergeStart);
          int delCount = info.getDelCount();
          if (delCount > maxDelCount) {
            expungeCandidate = mergeStart;
            maxDelCount = delCount;
          }
        }
      }
      mergeEnd = mergeStart;
    }

    if (partialExpunge && maxDelCount > 0) {
      // expunge deletes
      spec.add(new OneMerge(infos.asList().subList(expungeCandidate, expungeCandidate + 1)));
    }

    return spec;
  }

  private double[][] createVarianceTable(SegmentInfos infos, int last, int maxNumSegments)
      throws IOException {
    int maxMergeSegments = last - maxNumSegments + 1;
    double[][] variance = new double[last][maxMergeSegments];

    // compute the optimal segment size
    long optSize = 0;
    long[] sizeArr = new long[last];
    for (int i = 0; i < sizeArr.length; i++) {
      sizeArr[i] = size(infos.info(i));
      optSize += sizeArr[i];
    }
    optSize = (optSize / maxNumSegments);

    for (int i = 0; i < last; i++) {
      long size = 0;
      for (int j = 0; j < maxMergeSegments; j++) {
        if ((i + j) < last) {
          size += sizeArr[i + j];
          double residual = ((double) size / (double) optSize) - 1.0d;
          variance[i][j] = residual * residual;
        } else {
          variance[i][j] = Double.NaN;
        }
      }
    }
    return variance;
  }

  /**
   * Finds merges necessary to expunge all deletes from the
   * index. The number of large segments will stay the same.
   */
  @Override
  public MergeSpecification findForcedDeletesMerges(SegmentInfos infos)
      throws CorruptIndexException, IOException {
    final int numSegs = infos.size();
    final int numLargeSegs = (numSegs < _numLargeSegments ? numSegs : _numLargeSegments);
    MergeSpecification spec = null;

    if (numLargeSegs < numSegs) {
      List<SegmentInfo> smallSegmentList = infos.asList().subList(numLargeSegs, numSegs);
      SegmentInfos smallSegments = new SegmentInfos();
      smallSegments.addAll(smallSegmentList);
      spec = super.findForcedDeletesMerges(smallSegments);
    }

    if (spec == null) spec = new MergeSpecification();
    for (int i = 0; i < numLargeSegs; i++) {
      SegmentInfo info = infos.info(i);
      if (info.hasDeletions()) {
        spec.add(new OneMerge(infos.asList().subList(i, i + 1)));
      }
    }
    return spec;
  }

  /** Checks if any merges are now necessary and returns a
   *  {@link MergePolicy.MergeSpecification} if so.
   *  This merge policy try to maintain {@link
   *  #setNumLargeSegments} of large segments in similar sizes.
   *  {@link LogByteSizeMergePolicy} to small segments.
   *  Small segments are merged and promoted to a large segment
   *  when the total size reaches the average size of large segments.
   */
  @Override
  public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos)
      throws IOException {
    final int numSegs = infos.size();
    final int numLargeSegs = _numLargeSegments;

    if (numSegs <= numLargeSegs) return null;

    long totalLargeSegSize = 0;
    long totalSmallSegSize = 0;
    SegmentInfo info;

    // compute the total size of large segments
    for (int i = 0; i < numLargeSegs; i++) {
      info = infos.info(i);
      totalLargeSegSize += size(info);
    }
    // compute the total size of small segments
    for (int i = numLargeSegs; i < numSegs; i++) {
      info = infos.info(i);
      totalSmallSegSize += size(info);
    }

    long targetSegSize = (totalLargeSegSize / (numLargeSegs - 1));
    if (targetSegSize <= totalSmallSegSize) {
      // the total size of small segments is big enough,
      // promote the small segments to a large segment and do balanced merge,

      if (totalSmallSegSize < targetSegSize * 2) {
        MergeSpecification spec = findBalancedMerges(infos, numLargeSegs, (numLargeSegs - 1),
          _partialExpunge);
        if (spec == null) spec = new MergeSpecification(); // should not happen
        spec.add(new OneMerge(infos.asList().subList(numLargeSegs, numSegs)));
        return spec;
      } else {
        return findBalancedMerges(infos, numSegs, numLargeSegs, _partialExpunge);
      }
    } else if (_maxSegments < numSegs) {
      // we have more than _maxSegments, merge small segments smaller than targetSegSize/4
      MergeSpecification spec = new MergeSpecification();
      int startSeg = numLargeSegs;
      long sizeThreshold = (targetSegSize / 4);
      while (startSeg < numSegs) {
        info = infos.info(startSeg);
        if (size(info) < sizeThreshold) break;
        startSeg++;
      }
      spec.add(new OneMerge(infos.asList().subList(startSeg, numSegs)));
      return spec;
    } else {
      // apply the log merge policy to small segments.
      List<SegmentInfo> smallSegmentList = infos.asList().subList(numLargeSegs, numSegs);
      SegmentInfos smallSegments = new SegmentInfos();
      smallSegments.addAll(smallSegmentList);
      MergeSpecification spec = super.findMerges(mergeTrigger, smallSegments);
      if (_partialExpunge) {
        OneMerge expunge = findOneSegmentToExpunge(infos, numLargeSegs);
        if (expunge != null) {
          if (spec == null) spec = new MergeSpecification();
          spec.add(expunge);
        }
      }
      return spec;
    }
  }

  private OneMerge findOneSegmentToExpunge(SegmentInfos infos, int maxNumSegments)
      throws IOException {
    int expungeCandidate = -1;
    int maxDelCount = 0;

    for (int i = maxNumSegments - 1; i >= 0; i--) {
      SegmentInfo info = infos.info(i);
      int delCount = info.getDelCount();
      if (delCount > maxDelCount) {
        expungeCandidate = i;
        maxDelCount = delCount;
      }
    }
    if (maxDelCount > 0) {
      return new OneMerge(infos.asList().subList(expungeCandidate, expungeCandidate + 1));
    }
    return null;
  }

  public static class MergePolicyParams {
    public static final Logger log = Logger.getLogger(ZoieMergePolicy.MergePolicyParams.class
        .getName());
    private int _numLargeSegments;
    private int _maxSmallSegments;
    private boolean _doPartialExpunge;
    private int _mergeFactor;
    private boolean _useCompoundFile;
    private int _maxMergeDocs;

    public MergePolicyParams() {
      _useCompoundFile = false;
      _doPartialExpunge = false;
      _numLargeSegments = DEFAULT_NUM_LARGE_SEGMENTS;
      _maxSmallSegments = DEFAULT_NUM_SMALL_SEGMENTS;// 2 * LogMergePolicy.DEFAULT_MERGE_FACTOR;
      _mergeFactor = DEFAULT_MERGE_FACTOR;// LogMergePolicy.DEFAULT_MERGE_FACTOR;
      _maxMergeDocs = LogMergePolicy.DEFAULT_MAX_MERGE_DOCS;
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("useCompoundFile: ").append(_useCompoundFile);
      sb.append(", doPartialExpunge: ").append(_doPartialExpunge);
      sb.append(", numLargeSegments: ").append(_numLargeSegments);
      sb.append(", maxSmallSegments: ").append(_maxSmallSegments);
      sb.append(", mergeFactor: ").append(_mergeFactor);
      sb.append(", maxMergeDocs: ").append(_maxMergeDocs);
      return sb.toString();
    }

    public synchronized void setNumLargeSegments(int numLargeSegments) {
      if (numLargeSegments < 2) {
        log.warn("numLargeSegments cannot be less than 2, while " + numLargeSegments
            + " is requested. Override with 2.");
        numLargeSegments = 2;
      }
      _numLargeSegments = numLargeSegments;
      log.info(this.toString());
    }

    public synchronized int getNumLargeSegments() {
      return _numLargeSegments;
    }

    public synchronized void setMaxSmallSegments(int maxSmallSegments) {
      if (maxSmallSegments < getMergeFactor() + 1) {
        log.warn("MergeFactor is "
            + getMergeFactor()
            + ". maxSmallSegments is requested to be: "
            + maxSmallSegments
            + ". Override with mergeFactor + 1, since maxSmallSegments has to be greater than mergeFactor.");
        maxSmallSegments = getMergeFactor() + 1;
      }
      _maxSmallSegments = maxSmallSegments;
      log.info(this.toString());
    }

    public synchronized int getMaxSmallSegments() {
      return _maxSmallSegments;
    }

    public synchronized void setPartialExpunge(boolean doPartialExpunge) {
      _doPartialExpunge = doPartialExpunge;
      log.info(this.toString());
    }

    public synchronized boolean getPartialExpunge() {
      return _doPartialExpunge;
    }

    public synchronized void setMergeFactor(int mergeFactor) {
      if (mergeFactor < 2) {
        log.warn("mergeFactor has to be at least 2. Override " + mergeFactor + " with 2");
        mergeFactor = 2;
      }
      _mergeFactor = mergeFactor;
      if (_maxSmallSegments < getMergeFactor()) {
        log.warn("maxSmallSegments has to be greater than mergeFactor. Override maxSmallSegments to: "
            + (mergeFactor + 1));
        _maxSmallSegments = getMergeFactor() + 1;
      }
      log.info(this.toString());
    }

    public synchronized int getMergeFactor() {
      return _mergeFactor;
    }

    public synchronized void setMaxMergeDocs(int maxMergeDocs) {
      _maxMergeDocs = maxMergeDocs;
      log.info(this.toString());
    }

    public synchronized int getMaxMergeDocs() {
      return _maxMergeDocs;
    }

    public synchronized void setUseCompoundFile(boolean useCompoundFile) {
      _useCompoundFile = useCompoundFile;
      log.info(this.toString());
    }

    public synchronized boolean isUseCompoundFile() {
      return _useCompoundFile;
    }
  }
}
