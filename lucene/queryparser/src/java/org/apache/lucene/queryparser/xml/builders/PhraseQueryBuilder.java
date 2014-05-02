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

import org.w3c.dom.Element;

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
        
        try {
            TokenStream source = analyzer.tokenStream(field, phrase);
            source.reset();

            CachingTokenFilter buffer = new CachingTokenFilter(source);
            buffer.reset();// rewind the buffer stream

            TermToBytesRefAttribute termAtt = null;
            PositionIncrementAttribute posIncrAtt = null;

            if (buffer.hasAttribute(TermToBytesRefAttribute.class)) {
                termAtt = buffer.getAttribute(TermToBytesRefAttribute.class);
            }

            if (buffer.hasAttribute(PositionIncrementAttribute.class)) {
                posIncrAtt = buffer
                        .getAttribute(PositionIncrementAttribute.class);
            }

            BytesRef bytes = termAtt == null ? null : termAtt.getBytesRef();

            int position = -1;

            int positionIncrement = 1;
            if (posIncrAtt != null) {
                positionIncrement = posIncrAtt.getPositionIncrement();
            }

            while (buffer.incrementToken()) {
                position += positionIncrement;
                termAtt.fillBytesRef();
                pq.add(new Term(field, BytesRef.deepCopyOf(bytes)), position);
            }

            buffer.reset();
            source.end();
            source.close();// close original stream
        } catch (IOException ioe) {
            ParserException p = new ParserException(
                    "Cannot build phrase query from xml input. field:" + field
                            + ", phrase:" + phrase);
            p.initCause(ioe);
            throw p;
        }
        pq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
        // TODO pq.setSlop(phraseSlop);

        return pq;

    }

}
