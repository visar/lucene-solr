package org.apache.lucene.queryparser.xml.builders;

import org.apache.lucene.index.Term;
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
 * Builder for {@link TermQuery}
 */
public class TermQueryBuilder implements QueryBuilder {

  protected final TermBuilder termBuilder;

  public TermQueryBuilder(TermBuilder termBuilder) {
    this.termBuilder = termBuilder;
  }

  private class TermQueryProcessor implements TermBuilder.TermProcessor {
    private final float boost;
    TermQueryProcessor(final float boost) {
      this.boost = boost;
    }
    public TermQuery tq = null;
    public void process(Term t) throws ParserException {
      if (null == tq) {
        tq = new TermQuery(t);        
        tq.setBoost(boost);
      } else {
        throw new ParserException("TermQuery already set: " + tq);        
      }
    }          
  }
  
  @Override
  public Query getQuery(Element e) throws ParserException {
    
    TermQueryProcessor tp = new TermQueryProcessor(DOMUtils.getAttribute(e, "boost", 1.0f));
    
    termBuilder.extractTerms(tp, e);
    
    return tp.tq;
  }

}
