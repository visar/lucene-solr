package org.apache.lucene.queries;

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
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IntegerRange;
import org.apache.lucene.search.TermFreqDocIdSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A filter that includes documents that match a specific term
 * <code>IntegerRange.min</code> to <code>IntegerRange.max</code> times.
 */
final public class TermFreqFilter extends Filter {
  
  private final TermFilter termFilter;
  private final IntegerRange termFreqRange;
  
  /**
   * Construct a <code>TermFreqFilter</code>.
   * 
   * @param termFilter
   *          The TermFilter to which term frequency filtering should be applied.
   * @param termFreqRange
   *          The term frequency range filter to apply.
   */  
  public TermFreqFilter(TermFilter termFilter, IntegerRange termFreqRange) {
    this.termFilter = termFilter;
    this.termFreqRange = termFreqRange;
  }
  
  /**
   * @return The term this filter includes documents with.
   */
  public Term getTerm() {
    return termFilter.getTerm();
  }
  
  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, final Bits acceptDocs) throws IOException {
    final DocsEnum docsEnum = termFilter.getDocsEnum(context, acceptDocs, DocsEnum.FLAG_FREQS);
    
    return new DocIdSet() {
      @Override
      public DocIdSetIterator iterator() throws IOException {
        if (null == docsEnum) return null;

        return new DocIdSetIterator() {
          private final TermFreqDocIdSetIterator tfds_it = new TermFreqDocIdSetIterator(docsEnum, TermFreqFilter.this.termFreqRange);
          @Override
          public int advance(int target) throws IOException { return tfds_it.customAdvance(target); }
          @Override
          public int nextDoc() throws IOException { return tfds_it.customNextDoc(); }
          @Override
          public int docID() { return docsEnum.docID(); }
          @Override
          public long cost() { return docsEnum.cost(); }
        };
      }
    };
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    
    TermFreqFilter that = (TermFreqFilter) o;
    
    if (termFilter != null ? !termFilter.equals(that.termFilter) : that.termFilter != null) return false;
    if (termFreqRange != null ? !termFreqRange.equals(that.termFreqRange) : that.termFreqRange != null) return false;
    
    return true;
  }
  
  @Override
  public int hashCode() {
    return (termFilter != null ? termFilter.hashCode() : 0) ^ (termFreqRange != null ? termFreqRange.hashCode() : 0);
  }
  
  @Override
  public String toString() {
    return termFilter+"@TF="+termFreqRange;
  }
  
}
