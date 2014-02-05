package org.apache.lucene.queryparser.xml.builders;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
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
 * Builder for {@link TermFilter}
 */
public class TermFilterBuilder implements FilterBuilder {

  protected final Analyzer analyzer;

  public TermFilterBuilder() {
    this.analyzer = null;
  }

  public TermFilterBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public Filter getFilter(Element e) throws ParserException {
    String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    String value = DOMUtils.getNonBlankTextOrFail(e);
    if (null == analyzer) {
      return new TermFilter(new Term(field, value));
    } else {
      try {
        TokenStream ts = analyzer.tokenStream(field, value);
        TermToBytesRefAttribute termsRefAtt = ts
            .addAttribute(TermToBytesRefAttribute.class);
        BytesRef bytes = termsRefAtt.getBytesRef();
        ts.reset();
        
        TermFilter tf = null;
        if (ts.incrementToken()) {
          termsRefAtt.fillBytesRef();
          tf = new TermFilter(new Term(field, BytesRef.deepCopyOf(bytes)));
        }
        ts.close();
        return tf;
      } catch (IOException ioe) {
        throw new ParserException("IOException parsing value:" + value);
      }    
    }
  }
  
}
