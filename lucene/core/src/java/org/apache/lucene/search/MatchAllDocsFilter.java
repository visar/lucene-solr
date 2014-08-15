package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.Bits;

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

public class MatchAllDocsFilter extends Filter {

  class MatchAllDocIdSet extends DocIdSet {
    final int maxDocs;
    
    public MatchAllDocIdSet(int maxDocs) {
      if (maxDocs < 0)
        maxDocs = 0;
      this.maxDocs = maxDocs;
    }
    
    @Override
    public DocIdSetIterator iterator() throws IOException {
      return new MatchAllDocIdSetIterator(maxDocs);
    }
    
    class MatchAllDocIdSetIterator extends DocIdSetIterator {
      final int maxDocs;
      int iCurrent;
      public MatchAllDocIdSetIterator(int maxDocs) {
        this.maxDocs = maxDocs;
        iCurrent = -1;
      }

      @Override
      public int docID() {
        return iCurrent;
      }

      @Override
      public int nextDoc() throws IOException {
        if(++iCurrent < maxDocs)
          return iCurrent;
        else
          return NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) throws IOException {
        if(iCurrent < target && target < maxDocs) {
          iCurrent = target;
          return iCurrent;
        }
        else
          return NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 0;
      }
      
    }
    
  }

  @Override
  public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs)
      throws IOException {
    return new MatchAllDocIdSet(context.reader().maxDoc());
  }
  
}
