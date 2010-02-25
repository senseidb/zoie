
package proj.zoie.api;

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

/**
 * Maps a UID to the internal docid.
 */
public interface DocIDMapper
{
  /**
   * doc id not found indicator
   */
  public static final int NOT_FOUND = -1;
  /**
   * maps uid to a lucene docid
   * @param uid UID to be mapped.
   * @return {@link #NOT_FOUND} if uid is not found
   */
  int getDocID(long uid);
}
