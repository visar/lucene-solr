/**
 * 
 */
package org.apache.lucene.queryparser.xml;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.TermBuilder.TermProcessor;

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
 * This class will ensure single Term is extracted for a given field value 
 */
public class SingleTermProcessor implements TermProcessor {
  private Term term = null;
  private boolean multipleTerms = false;
  
  @Override
  public void process(Term t) {
    if (term == null) {
      term = t;
    } else {
      multipleTerms = true;
    }
  }
  public Term getTerm() throws ParserException {
    if(!multipleTerms && term != null){
      return term;
    }
    else if (term == null) {
      throw new ParserException("Empty term found.");
    }
    else {
      throw new ParserException("Multiple terms found.");
    }
  }
}

