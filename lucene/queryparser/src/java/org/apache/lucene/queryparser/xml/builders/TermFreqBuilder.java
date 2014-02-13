package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermFreqFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.queries.TermsFreqFilter;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IntegerRange;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermFreqQuery;
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

public class TermFreqBuilder implements QueryBuilder, FilterBuilder {
  
  private final FilterBuilder filterBuilder;
  private final QueryBuilder queryBuilder;
  
  
  public TermFreqBuilder(FilterBuilder filterBuilder, QueryBuilder queryBuilder) {
    this.filterBuilder = filterBuilder;
    this.queryBuilder = queryBuilder;
  }
  
  @Override
  public Query getQuery(Element e) throws ParserException {
    return build(queryBuilder.getQuery(e), e);
  }
  
  @Override
  public Filter getFilter(final Element e) throws ParserException {
    return build(filterBuilder.getFilter(e), e);
  }
  
  private IntegerRange getTF(Element e) {
    String minTF_str = DOMUtils.getAttribute(e, "minTF", null);
    String maxTF_str = DOMUtils.getAttribute(e, "maxTF", null);
    
    return new IntegerRange(
        (minTF_str == null ? null : Integer.parseInt(minTF_str)),
        (maxTF_str == null ? null : Integer.parseInt(maxTF_str)));
  }
  
  private Filter build(Filter f, Element e) throws ParserException {
    if (f instanceof TermFilter) {
      
      return new TermFreqFilter((TermFilter)f, getTF(e));
      
    } else if (f instanceof TermsFilter) {
      
      return new TermsFreqFilter((TermsFilter)f, getTF(e));
      
    } else {
      
      throw new ParserException("Filter is of unsupported type: "+f);
      
    }
  }
  
  private Query build(Query q, Element e) throws ParserException {
    IntegerRange termFreqRange = getTF(e);
    
    if (q instanceof TermQuery) {
      
      return new TermFreqQuery((TermQuery)q, termFreqRange);
      
    } else if (q instanceof BooleanQuery) {
      
      BooleanQuery bq = (BooleanQuery)q;
      for (BooleanClause bc : bq.clauses()) {
        Query subq = bc.getQuery();
        if (subq instanceof TermQuery) {
          bc.setQuery( new TermFreqQuery((TermQuery)subq, termFreqRange) );
        } else {
          throw new ParserException("Sub-Query is of unsupported type: "+subq);
        }
      }
      return bq;
      
    } else {
      
      throw new ParserException("Query is of unsupported type: "+q);
      
    }
  }

}
