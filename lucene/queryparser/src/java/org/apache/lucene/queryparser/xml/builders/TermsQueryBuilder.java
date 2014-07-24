package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.queryparser.xml.TermBuilder;
import org.w3c.dom.Element;

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

/**
 * Builds a BooleanQuery from all of the terms found in the XML element using the choice of analyzer
 */
public class TermsQueryBuilder implements QueryBuilder {

  private final TermBuilder termBuilder;

  public TermsQueryBuilder(TermBuilder termBuilder) {
    this.termBuilder = termBuilder;
  }

  private class TermsQueryProcessor implements TermBuilder.TermProcessor {
    private final BooleanQuery bq;
    TermsQueryProcessor(BooleanQuery bq) {
      this.bq = bq;
    }
    public void process(Term t) throws ParserException {
      bq.add(new BooleanClause(new TermQuery(t), BooleanClause.Occur.SHOULD));
    }
  }

  @Override
  public Query getQuery(Element e) throws ParserException {

    final BooleanQuery bq = new BooleanQuery(DOMUtils.getAttribute(e, "disableCoord", false));
    bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(e, "minimumNumberShouldMatch", 0));
    bq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
    final TermsQueryProcessor tp = new TermsQueryProcessor(bq);

    termBuilder.extractTerms(tp, e);

    return ((tp.bq.clauses().size() == 1) ? tp.bq.clauses().iterator().next().getQuery() : tp.bq);
  }

}
