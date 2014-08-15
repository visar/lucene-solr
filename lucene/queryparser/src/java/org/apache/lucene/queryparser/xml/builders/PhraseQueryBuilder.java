package org.apache.lucene.queryparser.xml.builders;

import java.io.IOException;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
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

public class PhraseQueryBuilder implements QueryBuilder {

    protected Analyzer analyzer;

    public PhraseQueryBuilder(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public Query getQuery(Element e) throws ParserException {
        String field = DOMUtils.getAttributeWithInheritanceOrFail(e,
                "fieldName");
        String phrase = DOMUtils.getNonBlankTextOrFail(e);

        PhraseQuery pq = new PhraseQuery();

        TokenStream source = null;
        try {
            source = analyzer.tokenStream(field, phrase);
            source.reset();

            TermToBytesRefAttribute termAtt = null;
            BytesRef bytes = null;
            if (source.hasAttribute(TermToBytesRefAttribute.class)) {
                termAtt = source.getAttribute(TermToBytesRefAttribute.class);
                bytes = termAtt.getBytesRef();
            }
            else throw new ParserException("Cannot build phrase query, "
                + "token stream has no TermToBytesRefAttribute. field:" + field
                + ", phrase:" + phrase);

            int positionIncrement = 1;
            if (source.hasAttribute(PositionIncrementAttribute.class)) {
                PositionIncrementAttribute posIncrAtt = source.getAttribute(PositionIncrementAttribute.class);
                positionIncrement = posIncrAtt.getPositionIncrement();
                if (positionIncrement <= 0) positionIncrement = 1;
            }

            int position = -1;
            while (source.incrementToken()) {
                position += positionIncrement;
                termAtt.fillBytesRef();
                pq.add(new Term(field, BytesRef.deepCopyOf(bytes)), position);
            }

            source.end();
        } catch (IOException ioe) {
            ParserException p = new ParserException(
                    "Cannot build phrase query from xml input. field:" + field
                            + ", phrase:" + phrase);
            p.initCause(ioe);
            throw p;
        } finally {
            IOUtils.closeWhileHandlingException(source);
        }
        
        if(pq.isEmpty())
        {
          throw new ParserException("Empty phrase query generated for field:" + field
                            + ", phrase:" + phrase);
        }
        pq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
        // TODO pq.setSlop(phraseSlop);
        return pq;

    }

}
