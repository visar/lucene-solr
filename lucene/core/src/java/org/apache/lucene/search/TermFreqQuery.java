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
import java.util.Set;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;

/** A Query that matches documents containing a term
 * <code>IntegerRange.min</code> to <code>IntegerRange.max</code> times.
 * This may be combined with other terms with a {@link BooleanQuery}.
 */
public class TermFreqQuery extends Query {

  private final TermQuery termQuery;
  private final IntegerRange termFreqRange;

  /**
   * Construct a <code>TermFreqQuery</code>.
   *
   * @param termQuery
   *          The TermQuery to which term frequency filtering should be applied.
   * @param termFreqRange
   *          The term frequency range filter to apply.
   */
  public TermFreqQuery(TermQuery termQuery, IntegerRange termFreqRange) {
    this.termQuery = termQuery;
    this.termFreqRange = termFreqRange;
  }

  /** Returns the term of this query. */
  public Term getTerm() { return termQuery.getTerm(); }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {

    final Weight weight = termQuery.createWeight(searcher);

    return new Weight() {
      public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
        return weight.explain(context, doc);
      }
      public Query getQuery() {
        return weight.getQuery();
      };
      public float getValueForNormalization() throws IOException {
        return weight.getValueForNormalization();
      };
      public void normalize(float norm, float topLevelBoost) {
        weight.normalize(norm, topLevelBoost);
      }
      public boolean scoresDocsOutOfOrder() {
        return weight.scoresDocsOutOfOrder();
      }

      @Override
      public Scorer scorer(AtomicReaderContext context, PostingFeatures flags, Bits acceptDocs) throws IOException {
        Scorer ws = weight.scorer(context, flags, acceptDocs);
        if (null == ws) {
          return null;
        }
        return new TermFreqScorer(ws, termFreqRange);
      }
    };
  }

  @Override
  public void extractTerms(Set<Term> terms) {
    termQuery.extractTerms(terms);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString(String field) {
    return termQuery+"@TF="+termFreqRange;
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TermFreqQuery))
      return false;
    TermFreqQuery other = (TermFreqQuery)o;

    if (termQuery != null ? !termQuery.equals(other.termQuery) : other.termQuery != null) return false;
    if (termFreqRange != null ? !termFreqRange.equals(other.termFreqRange) : other.termFreqRange != null) return false;

    return true;

  }

  /** Returns a hash code value for this object.*/
  @Override
  public int hashCode() {
    return (termQuery != null ? termQuery.hashCode() : 0) ^ (termFreqRange != null ? termFreqRange.hashCode() : 0);
  }

}
