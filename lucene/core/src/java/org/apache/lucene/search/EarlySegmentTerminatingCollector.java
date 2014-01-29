package org.apache.lucene.search;

/*
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

import org.apache.lucene.index.AtomicReaderContext;

/**
 * A {@link Collector} that early terminates collection of documents on a
 * per-segment basis.
 * @lucene.experimental
 */
public abstract class EarlySegmentTerminatingCollector extends Collector {

  protected final Collector in;
  
  private int numDocsToCollect;
  protected int getNumDocsToCollect() { return numDocsToCollect; }
  protected void setNumDocsToCollect(int val) { numDocsToCollect = val; }

  private int numCollected;
  protected int getNumCollected() { return numCollected; }

  /**
   * Create a new {@link EarlySegmentTerminatingCollector} instance.
   * 
   * @param in
   *          the collector to wrap
   */
  public EarlySegmentTerminatingCollector(Collector in) {
    this.in = in;
    this.numDocsToCollect = Integer.MAX_VALUE; // deriving classes are expected to change this (in their constructor and/or in setNextReader)
    this.numCollected = 0;
  }

  @Override
  public void collect(int doc) throws IOException {
    in.collect(doc);
    if (++numCollected >= numDocsToCollect) {
      throw new CollectionTerminatedException();
    }
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    in.setNextReader(context);
    numCollected = 0;
  }

}
