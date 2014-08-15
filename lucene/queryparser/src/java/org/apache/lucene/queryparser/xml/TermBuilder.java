package org.apache.lucene.queryparser.xml;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
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
    //callback for each term found during processing the field value
    public void process(Term t);
  };  

  //use this if you already have a field name and value and would like to apply analyzer and get Term/Terms.
  //TermProcess.process callback will be called for each term found
  public void extractTerms(TermProcessor tp, String field, String value) throws ParserException {
    if (null == analyzer) {
      tp.process(new Term(field, value));
    } else {
      TokenStream ts = null;
      try {
        ts = analyzer.tokenStream(field, value);
        TermToBytesRefAttribute termsRefAtt = ts
            .addAttribute(TermToBytesRefAttribute.class);
        BytesRef bytes = termsRefAtt.getBytesRef();
        ts.reset();

        while (ts.incrementToken()) {
          termsRefAtt.fillBytesRef();
          tp.process(new Term(field, BytesRef.deepCopyOf(bytes)));
        }
        ts.end();
      } catch (IOException ioe) {
        throw new ParserException("IOException parsing value:" + value);
      } finally {
        IOUtils.closeWhileHandlingException(ts);
      }
    }
  }

  //Handles nested Term TermQueries and/or not throws any exceptions on empty elements being found.
  public void extractTerms(TermProcessor tp, final Element e) throws ParserException {

    String fieldName = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");

    boolean called_extractTerms = false;

    final NodeList nl = e.getChildNodes();
    final int nl_len = nl.getLength();
    for (int i = 0; i < nl_len; i++) {
      final Node node = nl.item(i);
      if (Node.ELEMENT_NODE == node.getNodeType() && node.getNodeName().equals("Term")) {
        //Its analyzers job to ignore any spaces if required. ie. We don't do trimming before checking emptiness.
        String value = DOMUtils.getText((Element) node);
        if(value == null || value.length() == 0)
          return;
        extractTerms(
            tp,
            fieldName,
            value);
        called_extractTerms = true;
      }
    }

    if (!called_extractTerms) {
      String value = DOMUtils.getText(e);
      if(value == null || value.length() == 0)
        return;
      extractTerms(
          tp,
          fieldName,
          value);
    }
  }

}
