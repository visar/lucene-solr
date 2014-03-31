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

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>
 * <code>IntegerRange.min</code> to <code>IntegerRange.max</code> times.
 */
class TermFreqScorer extends Scorer {
  private final Scorer scorer;
  private final IntegerRange termFreqRange;
  private final TermFreqDocIdSetIterator scorer_tfdsit;

  /**
   * Construct a <code>TermFreqScorer</code>.
   *
   * @param scorer
   *          The score to which term frequency filtering should be applied.
   * @param termFreqRange
   *          The term frequency range filter to apply.
   */
  TermFreqScorer(Scorer scorer, IntegerRange termFreqRange) {
    super(scorer.weight);
    this.scorer = scorer;
    this.termFreqRange = termFreqRange;

    this.scorer_tfdsit = new TermFreqDocIdSetIterator(this.scorer, this.termFreqRange);
  }

  @Override
  public int docID() { return scorer.docID(); }

  @Override
  public int freq() throws IOException { return scorer.freq(); }

  @Override
  public int nextDoc() throws IOException { return scorer_tfdsit.customNextDoc(); }

  @Override
  public float score() throws IOException { return scorer.score(); }

  @Override
  public int advance(int target) throws IOException { return scorer_tfdsit.customAdvance(target); }

  @Override
  public long cost() { return scorer.cost(); }

  /** Returns a string representation of this <code>TermFreqScorer</code>. */
  @Override
  public String toString() { return scorer+"@TF="+termFreqRange; }
}
