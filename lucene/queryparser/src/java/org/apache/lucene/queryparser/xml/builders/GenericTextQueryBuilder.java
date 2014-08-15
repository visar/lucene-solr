package org.apache.lucene.queryparser.xml.builders;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

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
 * Builds a PhraseQuery if there are multiple terms.
 */
public class GenericTextQueryBuilder implements QueryBuilder {
  
  protected Analyzer analyzer;
  
  public GenericTextQueryBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
   
  @Override
  public Query getQuery(Element e) throws ParserException {
      String field = DOMUtils.getAttributeWithInheritanceOrFail(e,
              "fieldName");
      String text = DOMUtils.getText(e);

      PhraseQuery pq = null;//this will be instantiated only if the query results in multiple terms
      Term firstTerm = null;//Keeps the first Term in the query and if there are more terms found then this will be consumed by above PhraseQuery

      TokenStream source = null;
      try {
          source = analyzer.tokenStream(field, text);
          source.reset();

          TermToBytesRefAttribute termAtt = null;
          BytesRef bytes = null;
          if (source.hasAttribute(TermToBytesRefAttribute.class)) {
              termAtt = source.getAttribute(TermToBytesRefAttribute.class);
              bytes = termAtt.getBytesRef();
          }
          else throw new ParserException("Cannot build Text query, "
              + "token stream has no TermToBytesRefAttribute. field:" + field
              + ", phrase:" + text);

          int positionIncrement = 1;
          int position = -1;
          while (source.incrementToken()) {
              termAtt.fillBytesRef();
              Term t = new Term(field, BytesRef.deepCopyOf(bytes));
              if (null == firstTerm) {
                firstTerm = t;
                continue;
              }
              if (pq == null) {
                if (source.hasAttribute(PositionIncrementAttribute.class)) {
                PositionIncrementAttribute posIncrAtt = source.getAttribute(PositionIncrementAttribute.class);
                positionIncrement = posIncrAtt.getPositionIncrement();
                if (positionIncrement <= 0) positionIncrement = 1;
                }
                pq = new PhraseQuery();
                position += positionIncrement;
                pq.add(firstTerm, position);
              }
              position += positionIncrement;
              pq.add(t, position);
            }

          source.end();
      } catch (IOException ioe) {
          ParserException p = new ParserException(
                  "Cannot build generic text query from xml input. field:" + field
                          + ", text:" + text);
          p.initCause(ioe);
          throw p;
      } finally {
          IOUtils.closeWhileHandlingException(source);
      }

      if (firstTerm == null) {
        return new MatchAllDocsQuery();
      } else if (pq == null) {
          TermQuery tq = new TermQuery(firstTerm);
          tq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
          return tq;
      } else {
          pq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
          //TODO pq.setSlop(phraseSlop);
          return pq;
      }
  }
}
