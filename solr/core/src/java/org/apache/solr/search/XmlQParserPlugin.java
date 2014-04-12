package org.apache.solr.search;

import org.apache.lucene.queryparser.xml.CorePlusQueriesParser;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.search.Query;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.xml.SolrCoreParser;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;

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
