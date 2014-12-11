package org.apache.lucene.queryparser.xml.builders;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseNearQueryParser;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
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

/*builder to build complex phrase queries using interval implementation rather than Span*/
public class ComplexPhraseNearQueryBuilder implements QueryBuilder {
  
  protected Analyzer analyzer;
  
  public ComplexPhraseNearQueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  @Override
  public Query getQuery(Element e) throws ParserException {
      String field = DOMUtils.getAttributeWithInheritanceOrFail(e,
              "fieldName");
      String text = DOMUtils.getNonBlankTextOrFail(e);

      ComplexPhraseNearQueryParser parser = new ComplexPhraseNearQueryParser(Version.LUCENE_CURRENT, field, analyzer);
      
      Query q = null;
      try {
        q = parser.parse(text);
      } catch (ParseException pe){
        throw new ParserException("Error parsing ComplexPhraseNearQuery: " + text, pe);
      }
      return q;
  }
  
}
