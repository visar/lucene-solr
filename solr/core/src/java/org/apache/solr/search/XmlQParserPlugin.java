package org.apache.solr.search;

import org.apache.lucene.queryparser.xml.CorePlusQueriesParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Query;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

public class XmlQParserPlugin extends QParserPlugin {
  
  private static String contentEncoding = "UTF8";
  
  public void init(@SuppressWarnings("rawtypes") NamedList args) {
  }
  
  public QParser createParser(String qstr, SolrParams localParams,
      SolrParams params, SolrQueryRequest req) {
    return new XmlQParser(qstr, localParams, params, req);
  }
  
  class XmlQParser extends QParser {
    public XmlQParser(String qstr, SolrParams localParams,
        SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }
    
    public Query parse() throws SyntaxError {
      
      final String qstr = getString();
      if (qstr == null || qstr.length() == 0)
        return null;
      
      final IndexSchema schema = req.getSchema();
      SolrCoreParser solr_parser = new SolrCoreParser(
          req,
          new CorePlusQueriesParser(
              QueryParsing.getDefaultField(schema, getParam(CommonParams.DF)),
              schema.getQueryAnalyzer()));
      
      try {
        return solr_parser.parse(new ByteArrayInputStream(qstr.getBytes(contentEncoding)));
      } catch (UnsupportedEncodingException e) {
        throw new SyntaxError(e.getMessage() + " in " + req.toString());
      } catch (ParserException e) {
        throw new SyntaxError(e.getMessage() + " in " + req.toString());
      }
    }
  }
}
