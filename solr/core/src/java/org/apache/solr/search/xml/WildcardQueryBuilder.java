package org.apache.solr.search.xml;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.xml.DOMUtils;
import org.apache.lucene.queryparser.xml.ParserException;
import org.apache.lucene.queryparser.xml.QueryBuilder;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
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

public class WildcardQueryBuilder implements QueryBuilder
{
    protected IndexSchema schema;

    public WildcardQueryBuilder(IndexSchema schema) {
        this.schema = schema;
    }

    public Query getQuery(Element e) throws ParserException
    {
        String field = DOMUtils.getAttributeWithInheritanceOrFail(e, "fieldName");
        String value = DOMUtils.getNonBlankTextOrFail(e);

        SchemaField sf = schema.getFieldOrNull((field));
        FieldType ft = sf.getType();

        if (ft == null || !(ft instanceof TextField))
        {
            throw new ParserException("Wildcards are only supported on Text fields");
        }

        value = analyzeIfMultitermTermText(field,  value, ft);

        WildcardQuery wq = null;
        wq = new WildcardQuery(new Term(field, value));
        wq.setBoost(DOMUtils.getAttribute(e, "boost", 1.0f));
        return wq;
    }

    // Lifted from SolrQueryParserBase
    // This runs the KeywordTokenizer (to pull all the terms together)
    // and then the rest of the analyzer chain (to lower case, etc.)
    protected String analyzeIfMultitermTermText(String field, String part, FieldType fieldType)
    {
        if (part == null || !(fieldType instanceof TextField) || ((TextField) fieldType).getMultiTermAnalyzer() == null)
        {
            return part;
        }

        String out = TextField.analyzeMultiTerm(field, part, ((TextField) fieldType).getMultiTermAnalyzer())
                .utf8ToString();
        return out;
    }
}
