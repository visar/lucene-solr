package org.apache.solr.search.xml;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.SyntaxError;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

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

//<BoostedQuery>
//<Query>Main query goes here</Query>
//<Boosts><Boost>...</Boost><Boost>...</Boost></Boosts>
//</BoostedQuery>
//The score is multiplied by all the Boosts specified.
public class BoostedQueryBuilder implements QueryBuilder {

  private final QueryBuilder     factory;
  private final SolrQueryRequest solrQueryReq;

  public BoostedQueryBuilder(QueryBuilder factory,
      SolrQueryRequest req) {
    this.factory = factory;
    this.solrQueryReq = req;
  }

  @Override
  public Query getQuery(Element e) throws ParserException {

    try {
      Element mainQueryElem = DOMUtils.getChildByTagOrFail(e, "Query");
      mainQueryElem = DOMUtils.getFirstChildOrFail(mainQueryElem);
      Query mainQuery = factory.getQuery(mainQueryElem);
      Query topQuery = mainQuery;

      Element boostsElem = DOMUtils.getChildByTagOrFail(e, "Boosts");
      NodeList boostNodes = boostsElem.getChildNodes();
      List<ValueSource> boosts = new ArrayList<ValueSource>();

      for (int i = 0; i < boostNodes.getLength(); i++) {
        Node boostNode = boostNodes.item(i);
        if (boostNode != null
            && boostNode.getNodeName().equals("Boost")) {
          String boostStr = boostNode.getTextContent();
          if (boostStr == null || boostStr.length() == 0)
            continue;

          Query boost = new FunctionQParser(boostStr, null, null,
              solrQueryReq).getQuery();
          ValueSource vs;
          if (boost instanceof FunctionQuery) {
            vs = ((FunctionQuery) boost).getValueSource();
          } else {
            vs = new QueryValueSource(boost, 1.0f);
          }
          boosts.add(vs);
        }

        if (boosts.size() > 1) {
          ValueSource prod = new ProductFloatFunction(
              boosts.toArray(new ValueSource[boosts.size()]));
          topQuery = new BoostedQuery(mainQuery, prod);
        } else if (boosts.size() == 1) {
          topQuery = new BoostedQuery(mainQuery, boosts.get(0));
        }
      }
      return topQuery;
    } catch (SyntaxError ex) {
      throw new ParserException(ex);// until we fix the hack of using
      // FunctionQParser which is a separate
      // family of parser.
    }
  }
}
