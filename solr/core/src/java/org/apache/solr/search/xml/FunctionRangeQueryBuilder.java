package org.apache.solr.search.xml;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.FunctionRangeQuery;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.function.ValueSourceRangeFilter;
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

// This class has FunctionRangeFilterBuilder as a filter equivalent.
public class FunctionRangeQueryBuilder implements QueryBuilder {

  private final SolrQueryRequest solrQueryReq;

  public FunctionRangeQueryBuilder(SolrQueryRequest req) {
    this.solrQueryReq = req;
  }

  @Override
  public Query getQuery(Element e) throws ParserException {
    String lValue = DOMUtils.getAttribute(e, "l",null);
    String uValue = DOMUtils.getAttribute(e, "u",null);
    boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
    boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);

    String funcQuery = DOMUtils.getNonBlankTextOrFail(e);
    try {
      ValueSource vs;
      Query funcQ = new FunctionQParser(funcQuery, null, null,
          solrQueryReq).getQuery();
      if (funcQ instanceof FunctionQuery) {
        vs = ((FunctionQuery) funcQ).getValueSource();
      } else {
        vs = new QueryValueSource(funcQ, 0.0f);
      }

      ValueSourceRangeFilter rf = new ValueSourceRangeFilter(vs, lValue , uValue,
          lowerInclusive, upperInclusive);
      FunctionRangeQuery frq = new FunctionRangeQuery(rf);
      frq.setBoost(DOMUtils.getAttribute(e, "boost", 0.0f));
      return frq;
    } catch (SyntaxError ex) {
      throw new ParserException(ex);// until we fix the hack of using
      // FunctionQParser which is a separate
      // family of parser.
    }
  }
}
