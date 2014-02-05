package org.apache.solr.search;

import org.apache.lucene.queries.function.FunctionQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.QueryValueSource;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.FilterBuilder;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.function.ValueSourceRangeFilter;
import org.w3c.dom.Element;

// This class is the filter equivalent of FunctionRangeQueryBuilder query.
public class FunctionRangeFilterBuilder implements FilterBuilder {
  
  private final SolrQueryRequest solrQueryReq;
  
  public FunctionRangeFilterBuilder(SolrQueryRequest req) {
    this.solrQueryReq = req;
  }
  
  @Override
  public Filter getFilter(Element e) throws ParserException {
    String lValue = DOMUtils.getAttribute(e, "l",null);
    String uValue = DOMUtils.getAttribute(e, "u",null);
    boolean lowerInclusive = DOMUtils.getAttribute(e, "includeLower", true);
    boolean upperInclusive = DOMUtils.getAttribute(e, "includeUpper", true);
    
    String funcQuery = DOMUtils.getNonBlankTextOrFail(e);
    try {
      ValueSource vs;
      Query funcQ = new FunctionQParser(funcQuery, null, null,
          solrQueryReq).getQuery();
      if (funcQ instanceof FunctionQuery) {
        vs = ((FunctionQuery) funcQ).getValueSource();
      } else {
        vs = new QueryValueSource(funcQ, 0.0f);
      }
      
      ValueSourceRangeFilter rf = new ValueSourceRangeFilter(vs, lValue , uValue,
          lowerInclusive, upperInclusive);
      
      return rf;
      
    } catch (SyntaxError ex) {
      throw new ParserException(ex);// until we fix the hack of using
      // FunctionQParser which is a separate
      // family of parser.
    }
  }
}
