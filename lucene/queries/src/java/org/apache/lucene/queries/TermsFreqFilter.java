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

import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IntegerRange;
import org.apache.lucene.search.TermFreqDocIdSetIterator;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * Constructs a filter for docs matching any of the terms added to this class.
 * Unlike a RangeFilter this can be used for filtering on multiple terms that are not necessarily in
 * a sequence. An example might be a collection of primary keys from a database query result or perhaps
 * a choice of "category" labels picked by the end user. As a filter, this is much faster than the
 * equivalent query (a BooleanQuery with many "should" TermQueries)
 */
public final class TermsFreqFilter extends Filter {

  private final TermsFilter termsFilter;
  private final IntegerRange termFreqRange;

  /**
   * Construct a <code>TermsFreqFilter</code>.
   *
   * @param termsFilter
   *          The TermsFilter to which term frequency filtering should be applied.
   * @param termFreqRange
   *          The term frequency range filter to apply.
   */
  public TermsFreqFilter(TermsFilter termsFilter, IntegerRange termFreqRange) {
    this.termsFilter = termsFilter;
    this.termFreqRange = termFreqRange;
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
    return termsFilter.getDocIdSet(
        context,
        acceptDocs,
        DocsEnum.FLAG_FREQS,
        new TermsFilter.DocsEnumAdapter() {
          public DocsEnum get(final DocsEnum in) throws IOException {
            if (null == in) return null;

            return new DocsEnum() {
              private final TermFreqDocIdSetIterator tfds_it = new TermFreqDocIdSetIterator(in, TermsFreqFilter.this.termFreqRange);
              @Override
              public int advance(int target) throws IOException {
                return tfds_it.customAdvance(target);
              }
              @Override
              public int nextDoc() throws IOException {
                return tfds_it.customNextDoc();
              }
              @Override
              public int docID() {
                return in.docID();
              }
              @Override
              public long cost() {
                return in.cost();
              }
              @Override
              public int freq() throws IOException {
                return in.freq();
              }
            };
          }
        });
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (obj.getClass() != this.getClass())) {
      return false;
    }

    TermsFreqFilter test = (TermsFreqFilter) obj;

    if (termsFilter != null ? !termsFilter.equals(test.termsFilter) : test.termsFilter != null) return false;
    if (termFreqRange != null ? !termFreqRange.equals(test.termFreqRange) : test.termFreqRange != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (termsFilter != null ? termsFilter.hashCode() : 0) ^ (termFreqRange != null ? termFreqRange.hashCode() : 0);
  }

  @Override
  public String toString() {
    return termsFilter+"@TF="+termFreqRange;
  }

}
