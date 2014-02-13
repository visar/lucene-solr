package org.apache.lucene.queryparser.xml;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
 * Implemented by objects that produce Lucene Term objects from XML streams. Implementations are
 * expected to be thread-safe so that they can be used to simultaneously parse multiple XML documents.
 */
public class TermBuilder {
  
  private final Analyzer analyzer;

  public TermBuilder(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
  
  public interface TermProcessor {
    public void process(Term t) throws ParserException;
  };
  
  private void extractTerms(TermProcessor tp, String field, String value) throws ParserException {
    if (null == analyzer) {
      tp.process(new Term(field, value));
    } else {
      try {
        TokenStream ts = analyzer.tokenStream(field, value);
        TermToBytesRefAttribute termsRefAtt = ts
            .addAttribute(TermToBytesRefAttribute.class);
        BytesRef bytes = termsRefAtt.getBytesRef();
        ts.reset();
        
        while (ts.incrementToken()) {
          termsRefAtt.fillBytesRef();
          tp.process(new Term(field, BytesRef.deepCopyOf(bytes)));
        }
        ts.close();
      } catch (IOException ioe) {
        throw new ParserException("IOException parsing value:" + value);
      }    
    }
  }
  
  public void extractTerms(TermProcessor tp, final Element e) throws ParserException {
    
    String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
    
    boolean called_extractTerms = false;
    
    final NodeList nl = e.getChildNodes();
    for (int i = 0; i < nl.getLength(); i++) {
      final Node node = nl.item(i);
      if (Node.ELEMENT_NODE == node.getNodeType() && node.getNodeName().equals("Term")) {
        extractTerms(
            tp,
            fieldName,
            DOMUtils.getNonBlankTextOrFail((Element) node));
        called_extractTerms = true;
      }
    }
    
    if (!called_extractTerms) {
      extractTerms(
          tp,
          fieldName,
          DOMUtils.getNonBlankTextOrFail(e));
    }
  }
  
}
