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

import org.apache.lucene.index.AtomicReaderContext;

import java.io.IOException;

/**
 * The {@link TimingCollector} is used to time search requests.
 */
public class TimingCollector extends Collector {
  
  private Double startTime = null;
  private Collector collector;
  
  /**
   * Create a TimingCollector wrapper over another {@link Collector}.
   * @param collector the wrapped {@link Collector}
   */
  public TimingCollector(final Collector collector) {
    this.collector = collector;
  }
  
  private void setBaseline() {
    startTime = now();
  }
  
  /**
   *  Returns elapsed time (or 0 if baseline was not yet set).
   */
  public double elapsed() {
    if (startTime != null) {
      return now() - startTime;
    } else {
      return 0;
    }
  }
  
  /**
   * Get current time.
   * May override to implement a different timer (CPU time, etc).
   */
  protected double now() { return System.currentTimeMillis(); }
  
  @Override
  public void collect(final int doc) throws IOException {
    collector.collect(doc);
  }
  
  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    if (startTime == null) {
      setBaseline();
    }
    collector.setNextReader(context);
  }
  
  @Override
  public void setScorer(Scorer scorer) throws IOException {
    collector.setScorer(scorer);
  }
  
  @Override
  public boolean acceptsDocsOutOfOrder() {
    return collector.acceptsDocsOutOfOrder();
  }
  
  /**
   * This is so the same timer can be used with a multi-phase search process such as grouping. 
   * We don't want to create a new TimingCollector for each phase because that would 
   * reset the timer for each phase.
   *
   * @param collector The actual collector performing search functionality
   */
  public void setCollector(Collector collector) {
    this.collector = collector;
  }
  
}
