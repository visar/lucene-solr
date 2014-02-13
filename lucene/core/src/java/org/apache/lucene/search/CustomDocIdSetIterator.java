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

/**
 * This abstract class wraps a {@link DocIdSetIterator} object
 * and defines methods to iterate over its set of doc ids,
 * subject to user-defined {@link #match()}-ing criteria.
 */
public abstract class CustomDocIdSetIterator {

  /**
   * Returns whether or not the current document matches user-defined criteria.
   */
  public abstract boolean match() throws IOException;
  
  private final DocIdSetIterator ds_it;
  protected final DocIdSetIterator docIdSetIterator() { return ds_it; }
  
  public CustomDocIdSetIterator(DocIdSetIterator ds_it) {
    this.ds_it = ds_it;
  }

  /**
   * Advances to the next <code>match()</code>-ing document in the set and returns
   * the doc it is currently on, or <code>DocIdSetIterator.NO_MORE_DOCS</code> if
   * there are no more matching docs in the set.<br>
   * 
   * <b>NOTE:</b> after the iterator has exhausted you should not call this
   * method, as it may result in unpredicted behavior.
   */
  public final int customNextDoc() throws IOException {
    return customImpl(ds_it.nextDoc());
  }
  
  /**
   * Advances to the first <code>match()</code>-ing document beyond the current
   * whose document number is greater than or equal to <i>target</i>, and returns
   * the document number itself. 
   * Exhausts the iterator and returns <code>DocIdSetIterator.NO_MORE_DOCS</code>
   * if <i>target</i> is greater than the highest document number in the set.
   * <p>
   * 
   * Implemented via <code>DocIdSetIterator.advance(target)</code> and
   * <code>DocIdSetIterator.nextDoc()</code> calls.
   */
  public final int customAdvance(int target) throws IOException {
    return customImpl(ds_it.advance(target));
  }
  
  private final int customImpl(int doc) throws IOException {
    while (DocIdSetIterator.NO_MORE_DOCS != doc) {
      if (!match()) {
        doc = ds_it.nextDoc();
        continue;
      } else {
        break;
      }
    }
    return doc;
  }
  
}
