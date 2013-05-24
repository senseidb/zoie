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

public interface ZoieSystemAdminMBean extends ZoieAdminMBean {
  Date getLastOptimizationTime();

  void optimize(int numSegs) throws IOException;

  void purgeIndex() throws IOException;

  void expungeDeletes() throws IOException;

  void setUseCompoundFile(boolean useCompoundFile);

  int getDiskIndexSize();

  long getMinUID() throws IOException;

  long getMaxUID() throws IOException;

  public long getFreshness();

  public void setFreshness(long freshness);

  /**
   * @return a String representation of the search result given the string representations of query arguments.
   */
  public String search(String field, String query);

  /**
   * @return the a String representation of the Document(s) referred to by the given UID
   */
  public String getDocument(long UID);
}
