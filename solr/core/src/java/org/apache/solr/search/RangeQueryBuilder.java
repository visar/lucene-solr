package org.apache.solr.search;

import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;

import org.w3c.dom.Element;

public class RangeQueryBuilder implements QueryBuilder {

    protected final IndexSchema schema;

    public RangeQueryBuilder(IndexSchema schema) {
        this.schema = schema;
    }

    @Override
    public Query getQuery(Element e) throws ParserException {
        String field = DOMUtils.getAttributeWithInheritanceOrFail(e,
                "fieldName");
        String lowerTerm = DOMUtils.getAttribute(e, "lowerTerm",null);
        String upperTerm = DOMUtils.getAttribute(e, "upperTerm",null);
        boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
        boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);
        SchemaField sf = schema.getField(field);

        return sf.getType().getRangeQuery(null, sf, lowerTerm, upperTerm,
                lowerInclusive, upperInclusive);
        //the QParser instance is not really used inside getRangeQuery. So passing null.
    }

}
