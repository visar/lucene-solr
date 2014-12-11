package org.apache.lucene.queryparser.complexPhrase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.valuesource.ConstValueSource;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldedQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.intervals.FieldedBooleanQuery;
import org.apache.lucene.search.intervals.OrderedNearQuery;
import org.apache.lucene.util.Version;

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

public class ComplexPhraseNearQueryParser extends QueryParser{

  private void setRewriteMethodforMutitermQuery(Query q)
  {
    if((q instanceof PrefixQuery) || (q instanceof WildcardQuery))
    {
      ((MultiTermQuery)q).setRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_QUERY_REWRITE);
    }
    
  }
  
  @Override
  public Query parse(String queryText) throws ParseException {
    Query q = null;
    
    Query unknownQuery = super.parse(queryText);
    
    if ((unknownQuery instanceof TermQuery) || (unknownQuery instanceof PrefixQuery) || (unknownQuery instanceof WildcardQuery)) {
      setRewriteMethodforMutitermQuery(unknownQuery);
      q = unknownQuery;
    }
    else
    {
      if (!(unknownQuery instanceof BooleanQuery)) {
        throw new IllegalArgumentException("Unknown query type \""
            + unknownQuery.getClass().getName()
            + "\" found in phrase query string \"" + queryText
            + "\"");
      }
      BooleanQuery bq = (BooleanQuery) unknownQuery;
      BooleanClause[] bclauses = bq.getClauses();
      FieldedQuery[] subQueries = new FieldedQuery[bclauses.length];
      
      for (int i = 0; i < bclauses.length; i++) {
        subQueries[i] = FieldedBooleanQuery.toFieldedQuery(bclauses[i].getQuery());
        setRewriteMethodforMutitermQuery(subQueries[i]);
        }
      q = new OrderedNearQuery(0/*slop*/, subQueries);
    }
        
    return q;
  }

  public ComplexPhraseNearQueryParser(Version matchVersion, String f, Analyzer a) {
    super(matchVersion, f, a);
  }
  
}
