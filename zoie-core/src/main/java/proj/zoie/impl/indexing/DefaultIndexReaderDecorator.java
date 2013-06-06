package proj.zoie.impl.indexing;

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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;

import proj.zoie.api.ZoieIndexReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class DefaultIndexReaderDecorator implements IndexReaderDecorator<IndexReader> {

  @Override
  public IndexReader decorate(ZoieIndexReader<IndexReader> indexReader) throws IOException {
    return indexReader.getInnerReader();
  }

  @Override
  public IndexReader redecorate(IndexReader decorated, ZoieIndexReader<IndexReader> copy) throws IOException {
    return copy.getInnerReader();
  }

  @Override
  public void setDeleteSet(IndexReader reader, DocIdSet docIds) {
    // do nothing
  }

}
