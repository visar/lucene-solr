package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
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
 * Builds a MatchAllDocsQuery if the terms found are zero. 
 * Builds a TermQuery If there is only one resulting term after analyzer being applied 
 * Builds a BooleanQuery from all of the terms found in the XML element using the choice of analyzer, if there are multiple terms.
 */
public class TermsQueryBuilder implements QueryBuilder {

  private final TermBuilder termBuilder;

  public TermsQueryBuilder(TermBuilder termBuilder) {
    this.termBuilder = termBuilder;
  }

  private class TermsQueryProcessor implements TermBuilder.TermProcessor {
    private BooleanQuery bq = null;//this will be instantiated only if the TermsQuery results in multiple terms
    private TermQuery    firstTq = null;//Keeps the first TermQuery for the first Term in the query and if there are more terms found then this will be consumed by above BooleanQuery
    private final Element xmlQueryElement;
    
    TermsQueryProcessor(Element e) {
      xmlQueryElement = e;
    }
    public void process(Term t){
      if (null == firstTq) {
        firstTq = new TermQuery(t);
        return;
      }
      if (bq == null) {
        bq = new BooleanQuery(DOMUtils.getAttribute(xmlQueryElement, "disableCoord", false));
        bq.add(new BooleanClause(firstTq, BooleanClause.Occur.SHOULD));
      }
      bq.add(new BooleanClause(new TermQuery(t), BooleanClause.Occur.SHOULD));
    }
    
    public Query getQuery() {
      if (firstTq == null) {
          return new MatchAllDocsQuery();
      } else if (bq == null) {
          firstTq.setBoost(DOMUtils.getAttribute(xmlQueryElement, "boost", 1.0f));
          return firstTq;
      } else {
          bq.setBoost(DOMUtils.getAttribute(xmlQueryElement, "boost", 1.0f));
          bq.setMinimumNumberShouldMatch(DOMUtils.getAttribute(xmlQueryElement, "minimumNumberShouldMatch", 0));
          return bq;
      }
    }
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    final TermsQueryProcessor tp = new TermsQueryProcessor(e);
    termBuilder.extractTerms(tp, e);
    return tp.getQuery();
  }

}
