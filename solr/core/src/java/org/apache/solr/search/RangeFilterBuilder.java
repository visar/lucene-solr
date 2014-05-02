package org.apache.solr.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.NumericRangeFilter;
import org.apache.lucene.search.TermRangeFilter;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.TrieDateField;
import org.apache.solr.schema.TrieDoubleField;
import org.apache.solr.schema.TrieField;
import org.apache.solr.schema.TrieFloatField;
import org.apache.solr.schema.TrieIntField;
import org.apache.solr.schema.TrieLongField;

import org.w3c.dom.Element;
//Currently we have RangeFilter for numeric values and string. but need to specify precisionStep and type.
//No support for date types. THis filter aims to integrate all into one and need to find precisionStep type from schema.
//Look at RangeQueryBuilder for more info

public class RangeFilterBuilder implements FilterBuilder {

    private final IndexSchema schema;
    static final TrieDateField dateField = new TrieDateField();

    public RangeFilterBuilder(IndexSchema schema) {
        this.schema = schema;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Filter getFilter(Element e) throws ParserException {
        String field = DOMUtils.getAttributeWithInheritanceOrFail(e,
                "fieldName");
        String lowerTerm = DOMUtils.getAttribute(e, "lowerTerm", null);
        String upperTerm = DOMUtils.getAttribute(e, "upperTerm", null);
        boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
        boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);
        SchemaField sf = schema.getField(field);
        FieldType ft = sf.getType();

        Filter filter;
        try {
            if (ft instanceof TrieDateField) {
                // get the precision step
                int ps = ((TrieDateField) ft).getPrecisionStep();
                if (ps <= 0 || ps >= 64)
                    ps = Integer.MAX_VALUE;
                filter = NumericRangeFilter.newLongRange(
                        field,
                        ps,
                        lowerTerm == null ? null : dateField.parseMath(null,
                                lowerTerm).getTime(), upperTerm == null ? null
                                : dateField.parseMath(null, upperTerm)
                                        .getTime(), lowerInclusive,
                        upperInclusive);
            } else if (ft instanceof TrieField) {
                // get the precision step
                int ps = ((TrieField) ft).getPrecisionStep();
                if (ps <= 0 || ps >= 64)
                    ps = Integer.MAX_VALUE;

                if (ft instanceof TrieIntField) {
                    filter = NumericRangeFilter.newIntRange(field, ps,
                            Integer.parseInt(lowerTerm),
                            Integer.parseInt(upperTerm), lowerInclusive,
                            upperInclusive);
                } else if (ft instanceof TrieLongField) {
                    filter = NumericRangeFilter.newLongRange(field, ps,
                            Long.parseLong(lowerTerm),
                            Long.parseLong(upperTerm), lowerInclusive,
                            upperInclusive);
                } else if (ft instanceof TrieDoubleField) {
                    filter = NumericRangeFilter.newDoubleRange(field, ps,
                            Double.parseDouble(lowerTerm),
                            Double.parseDouble(upperTerm), lowerInclusive,
                            upperInclusive);
                } else if (ft instanceof TrieFloatField) {
                    filter = NumericRangeFilter.newFloatRange(field, ps,
                            Float.parseFloat(lowerTerm),
                            Float.parseFloat(upperTerm), lowerInclusive,
                            upperInclusive);
                } else {
                    throw new ParserException("Unknown TrieField type.");
                }
            } else {
                if (ft instanceof TextField) {
                    // Analyzer chain
                    Analyzer multiAnalyzer = ((TextField) ft)
                            .getMultiTermAnalyzer();
                    BytesRef lower = lowerTerm == null ? null : TextField
                            .analyzeMultiTerm(field, lowerTerm, multiAnalyzer);
                    BytesRef upper = upperTerm == null ? null : TextField
                            .analyzeMultiTerm(field, upperTerm, multiAnalyzer);
                    filter = new TermRangeFilter(field, lower, upper,
                            lowerInclusive, upperInclusive);
                } else {
                    filter = TermRangeFilter.newStringRange(field, lowerTerm,
                            upperTerm, lowerInclusive, upperInclusive);
                }
            }

            return filter;
        } catch (NumberFormatException nfe) {
            throw new ParserException(
                    "Could not parse lowerTerm or upperTerm into a number", nfe);
        }
    }
}
