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

package org.apache.solr.handler.component;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.util.SolrPluginUtils;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.solr.common.params.CommonParams.FQ;

/**
 * Adds debugging information to a request.
 * 
 *
 * @since solr 1.3
 */
public class DebugComponent extends SearchComponent
{
  public static final String COMPONENT_NAME = "debug";
  
  private static final String DISTRIB_TIMING = "distribTiming";
  
  private static final String SUBMIT_WAITING_TIME = "submitWaiting";
  private static final String ELAPSED_TIME = "elapsed";
  private static final String TAKE_WAITING_TIME = "takeWaiting";
  private static final String BREAKDOWN = "breakdown";
  
  /**
   * A counter to ensure that no RID is equal, even if they fall in the same millisecond
   */
  private static final AtomicLong ridCounter = new AtomicLong();
  
  /**
   * Map containing all the possible stages as key and
   * the corresponding readable purpose as value
   */
  private static final Map<Integer, String> stages;

  static {
      Map<Integer, String> map = new TreeMap<>();
      map.put(ResponseBuilder.STAGE_START, "START");
      map.put(ResponseBuilder.STAGE_PARSE_QUERY, "PARSE_QUERY");
      map.put(ResponseBuilder.STAGE_TOP_GROUPS, "TOP_GROUPS");
      map.put(ResponseBuilder.STAGE_EXECUTE_QUERY, "EXECUTE_QUERY");
      map.put(ResponseBuilder.STAGE_GET_FIELDS, "GET_FIELDS");
      map.put(ResponseBuilder.STAGE_DONE, "DONE");
      stages = Collections.unmodifiableMap(map);
  }
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException
  {
    if(rb.isDebugTrack() && rb.isDistrib) {
      doDebugTrack(rb);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(ResponseBuilder rb) throws IOException
  {
    if( rb.isDebug() ) {
      DocList results = null;
      //some internal grouping requests won't have results value set
      if(rb.getResults() != null) {
        results = rb.getResults().docList;
      }

      NamedList stdinfo = SolrPluginUtils.doStandardDebug( rb.req,
          rb.getQueryString(), rb.wrap(rb.getQuery()), results, rb.isDebugQuery(), rb.isDebugResults());
      
      NamedList info = rb.getDebugInfo();
      if( info == null ) {
        rb.setDebugInfo( stdinfo );
        info = stdinfo;
      }
      else {
        info.addAll( stdinfo );
      }
      
      if (rb.isDebugQuery() && rb.getQparser() != null) {
        rb.getQparser().addDebugInfo(rb.getDebugInfo());
      }
      
      if (null != rb.getDebugInfo() ) {
        if (rb.isDebugQuery() && null != rb.getFilters() ) {
          info.add("filter_queries",rb.req.getParams().getParams(FQ));
          List<String> fqs = new ArrayList<>(rb.getFilters().size());
          for (Query fq : rb.getFilters()) {
            fqs.add(QueryParsing.toString(fq, rb.req.getSchema()));
          }
          info.add("parsed_filter_queries",fqs);
        }
        
        // Add this directly here?
        rb.rsp.add("debug", rb.getDebugInfo() );
      }
    }
  }


  private void doDebugTrack(ResponseBuilder rb) {
    SolrQueryRequest req = rb.req;
    String rid = req.getParams().get(CommonParams.REQUEST_ID);
    if(rid == null || "".equals(rid)) {
      rid = generateRid(rb);
      ModifiableSolrParams params = new ModifiableSolrParams(req.getParams());
      params.add(CommonParams.REQUEST_ID, rid);//add rid to the request so that shards see it
      req.setParams(params);
    }
    rb.addDebug(rid, "track", CommonParams.REQUEST_ID);//to see it in the response
    rb.rsp.addToLog(CommonParams.REQUEST_ID, rid); //to see it in the logs of the landing core
    
  }
  
  private String generateRid(ResponseBuilder rb) {
    String hostName = rb.req.getCore().getCoreDescriptor().getCoreContainer().getHostName();
    return hostName + "-" + rb.req.getCore().getName() + "-" + System.currentTimeMillis() + "-" + ridCounter.getAndIncrement();
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    if (!rb.isDebug()) return;
    
    // Turn on debug to get explain only when retrieving fields
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
      sreq.purpose |= ShardRequest.PURPOSE_GET_DEBUG;
      if (rb.isDebugAll()) {
        sreq.params.set(CommonParams.DEBUG_QUERY, "true");
      } else {
        if (rb.isDebugQuery()){
          sreq.params.add(CommonParams.DEBUG, CommonParams.QUERY);
        }
        if (rb.isDebugResults()){
          sreq.params.add(CommonParams.DEBUG, CommonParams.RESULTS);
        }
      }
    } else {
      sreq.params.set(CommonParams.DEBUG_QUERY, "false");
      sreq.params.set(CommonParams.DEBUG, "false");
    }
    if (rb.isDebugTimings()) {
      sreq.params.add(CommonParams.DEBUG, CommonParams.TIMING);
    } 
    if (rb.isDebugTrack()) {
      sreq.params.add(CommonParams.DEBUG, CommonParams.TRACK);
      sreq.params.set(CommonParams.REQUEST_ID, rb.req.getParams().get(CommonParams.REQUEST_ID));
      sreq.params.set(CommonParams.REQUEST_PURPOSE, SolrPluginUtils.getRequestPurpose(sreq.purpose));
    }
  }

  private class Metric {
    ArrayList<Object> rawValues = new ArrayList<>();
    private long count = 0;
    private long sum = 0;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private final TimeUnit sourceUnit;
    
    Metric() {
      this.sourceUnit = null;
    }

    Metric(TimeUnit sourceUnit) {
      this.sourceUnit = sourceUnit;
    }

    void record(String shardAddress, long val) {
      if (shardAddress != null) {
        SimpleOrderedMap<Object> rv = new SimpleOrderedMap<>();
        rv.add("key", shardAddress);
        rv.add("val", val);
        rawValues.add(rv);
      }      
      count += 1;
      sum += val;
      if (val < min) {
        min = val;
      }
      if (val > max) {
        max = val;
      }
    }
    
    void add(final Metric other) {
      this.rawValues.addAll(other.rawValues);
      this.count += other.count;
      this.sum += other.sum;
      if (other.min < this.min) {
        this.min = other.min;
      }
      if (other.max > this.max) {
        this.max = other.max;
      }
    }

    Object toObject()
    {
      return toObject(sourceUnit);
    }
    
    Object toObject(TimeUnit destinationUnit)
    {
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      if (!rawValues.isEmpty()) {
        nl.add("rawValues", rawValues);
      }
      nl.add("count", count);
      if (sourceUnit != destinationUnit) {
        nl.add("sum", destinationUnit.convert(sum, sourceUnit));
        nl.add("min", destinationUnit.convert(min, sourceUnit));
        nl.add("max", destinationUnit.convert(max, sourceUnit));        
      } else {
        nl.add("sum", sum);
        nl.add("min", min);
        nl.add("max", max);
      }
      return nl;
    }    

    @Override
    public String toString()
    {
      return toObject().toString();
    }
  };
  
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (rb.isDebugTimings() && rb.stage > ResponseBuilder.STAGE_START) {
      Metric submitWaitingTime = new Metric(TimeUnit.NANOSECONDS);
      Metric elapsedTime = new Metric(TimeUnit.MILLISECONDS);
      Metric takeWaitingTime = new Metric(TimeUnit.NANOSECONDS);
      NamedList<Object> debug = null;
      
      for (ShardResponse srsp : sreq.responses) {
        final String shardAddress = (rb.isDebugRawTimings() ? srsp.getShardAddress() : null);
        submitWaitingTime.record(shardAddress, srsp.getSubmitWaitingTime());
        elapsedTime.record(shardAddress, srsp.getSolrResponse().getElapsedTime()); 
        takeWaitingTime.record(shardAddress, srsp.getTakeWaitingTime());            

        if (srsp.getException() != null) {
          // can't expect the debug content if there was an exception for this request
          // this should only happen when using shards.tolerant=true
          continue;
        }
        NamedList sdebug = (NamedList)srsp.getSolrResponse().getResponse().get("debug");
        debug = (NamedList)merge(sdebug, debug, EXCLUDE_SET, true, shardAddress);
      }
      
      if (debug != null) {
        debug = convertMetric(debug);
      }
      
      final String stage = stages.get(rb.stage);
      
      NamedList<Object> debug_info = rb.getDebugInfo();
      if (debug_info == null) {
        debug_info = new SimpleOrderedMap<>();
      }        
      
      NamedList<Object> distribTiming_info = (NamedList<Object>) debug_info.get(DISTRIB_TIMING);
      if (distribTiming_info == null) {
        debug_info.add(DISTRIB_TIMING, new SimpleOrderedMap<>());
        distribTiming_info = (NamedList<Object>) debug_info.get(DISTRIB_TIMING);
      }
      
      NamedList<Object> stage_info = (NamedList<Object>) distribTiming_info.get(stage);
      if (stage_info == null) {
        distribTiming_info.add(stage, new SimpleOrderedMap<>());
        stage_info = (NamedList<Object>) distribTiming_info.get(stage);
      }
      
      stage_info.add(SUBMIT_WAITING_TIME, submitWaitingTime.toObject(TimeUnit.MILLISECONDS));        
      stage_info.add(ELAPSED_TIME, elapsedTime.toObject(TimeUnit.MILLISECONDS));
      stage_info.add(TAKE_WAITING_TIME, takeWaitingTime.toObject(TimeUnit.MILLISECONDS));        
      stage_info.add(BREAKDOWN, debug);
      
      rb.setDebugInfo(debug_info);
    }
    
    if (rb.isDebugTrack() && rb.isDistrib && !rb.finished.isEmpty()) {
      @SuppressWarnings("unchecked")
      List<Object> stageList = (List<Object>) ((NamedList<Object>)rb.getDebugInfo().get("track")).get(stages.get(rb.stage));
      if(stageList == null) {
        stageList = new ArrayList<>(sreq.responses.size());
        rb.addDebug(stageList, "track", stages.get(rb.stage));
      }
      for(ShardResponse response: sreq.responses) {
        stageList.add(getTrackResponse(response));
      }
    }
  }

  private final static Set<String> EXCLUDE_SET = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("explain")));

  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.isDebug() && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      NamedList<Object> info = rb.getDebugInfo();
      NamedList<Object> explain = new SimpleOrderedMap<>();

      Map.Entry<String, Object>[]  arr =  new NamedList.NamedListEntry[rb.resultIds.size()];
      // Will be set to true if there is at least one response with PURPOSE_GET_DEBUG
      boolean hasGetDebugResponses = false;

      for (ShardRequest sreq : rb.finished) {
        for (ShardResponse srsp : sreq.responses) {
          if (srsp.getException() != null) {
            // can't expect the debug content if there was an exception for this request
            // this should only happen when using shards.tolerant=true
            continue;
          }
          NamedList sdebug = (NamedList)srsp.getSolrResponse().getResponse().get("debug");
          info = (NamedList)merge(sdebug, info, EXCLUDE_SET, false, null);
          if ((sreq.purpose & ShardRequest.PURPOSE_GET_DEBUG) != 0) {
            hasGetDebugResponses = true;
            if (rb.isDebugResults()) {
              NamedList sexplain = (NamedList)sdebug.get("explain");
              for (int i = 0; i < sexplain.size(); i++) {
                String id = sexplain.getName(i);
                // TODO: lookup won't work for non-string ids... String vs Float
                ShardDoc sdoc = rb.resultIds.get(id);
                int idx = sdoc.positionInResponse;
                arr[idx] = new NamedList.NamedListEntry<>(id, sexplain.getVal(i));
              }
            }
          }
        }
      }

      if (rb.isDebugResults()) {
         explain = SolrPluginUtils.removeNulls(arr, new SimpleOrderedMap<>());
      }

      if (!hasGetDebugResponses) {
        if (info == null) {
          info = new SimpleOrderedMap<>();
        }
        // No responses were received from shards. Show local query info.
        SolrPluginUtils.doStandardQueryDebug(
                rb.req, rb.getQueryString(),  rb.wrap(rb.getQuery()), rb.isDebugQuery(), info);
        if (rb.isDebugQuery() && rb.getQparser() != null) {
          rb.getQparser().addDebugInfo(info);
        }
      }
      if (rb.isDebugResults()) {
        int idx = info.indexOf("explain",0);
        if (idx>=0) {
          info.setVal(idx, explain);
        } else {
          info.add("explain", explain);
        }
      }

      rb.setDebugInfo(info);
      rb.rsp.add("debug", rb.getDebugInfo() );
    }
    
  }


  private NamedList<Object> getTrackResponse(ShardResponse shardResponse) {
    NamedList<Object> namedList = new SimpleOrderedMap<>();
    if (shardResponse.getException() != null) {
      namedList.add("Exception", shardResponse.getException().getMessage());
      return namedList;
    }
    namedList.add("shardAddress", shardResponse.getShardAddress());
    NamedList<Object> responseNL = shardResponse.getSolrResponse().getResponse();
    @SuppressWarnings("unchecked")
    NamedList<Object> responseHeader = (NamedList<Object>)responseNL.get("responseHeader");
    if(responseHeader != null) {
      namedList.add("QTime", responseHeader.get("QTime"));
    }
    namedList.add(SUBMIT_WAITING_TIME, shardResponse.getSubmitWaitingTime());            
    namedList.add(ELAPSED_TIME, shardResponse.getSolrResponse().getElapsedTime());
    namedList.add(TAKE_WAITING_TIME, shardResponse.getTakeWaitingTime());            
    namedList.add("RequestPurpose", shardResponse.getShardRequest().params.get(CommonParams.REQUEST_PURPOSE));
    SolrDocumentList docList = (SolrDocumentList)shardResponse.getSolrResponse().getResponse().get("response");
    if(docList != null) {
      namedList.add("NumFound", docList.getNumFound());
    }
    namedList.add("Response", String.valueOf(responseNL));
    return namedList;
  }

  NamedList<Object> convertMetric(NamedList<Object> info) {
    if (info != null) {
      for (int ii=0; ii<info.size(); ii++) {
        Object sval = info.getVal(ii);
        if (sval instanceof Metric) {
          info.setVal(ii, ((Metric) sval).toObject());
        }
        else if (sval instanceof NamedList) {
          info.setVal(ii, convertMetric((NamedList<Object>)sval));
        }
      }
    }
    return info;
  }
  
  Object merge(Object source, Object dest, Set<String> exclude, final boolean metrics, final String shardAddress) {
    if (source == null) return dest;
    if (dest == null) {
      if (source instanceof NamedList) {
        dest = source instanceof SimpleOrderedMap ? new SimpleOrderedMap() : new NamedList();
      } else {
        return source;
      }
    } else {

      if (dest instanceof Collection) {
        if (source instanceof Collection) {
          ((Collection)dest).addAll((Collection)source);
        } else {
          ((Collection)dest).add(source);
        }
        return dest;
      } else if (source instanceof Number) {
        if (dest instanceof Number) {
          if (source instanceof Double || dest instanceof Double) {
            return ((Number)source).doubleValue() + ((Number)dest).doubleValue();
          }
          return ((Number)source).longValue() + ((Number)dest).longValue();
        }
        else if (dest instanceof Metric) {
          ((Metric) dest).record(shardAddress, ((Number)source).longValue());
          return dest;
        }
        // fall through
      } else if (source instanceof String) {
        if (source.equals(dest)) {
          return dest;
        }
        // fall through
      } else if (source instanceof Metric && dest instanceof Metric) {
        ((Metric) dest).add(((Metric) source));
        return dest;
      }
    }


    if (source instanceof NamedList && dest instanceof NamedList) {
      NamedList<Object> tmp = new NamedList<>();
      @SuppressWarnings("unchecked")
      NamedList<Object> sl = (NamedList<Object>)source;
      @SuppressWarnings("unchecked")
      NamedList<Object> dl = (NamedList<Object>)dest;
      for (int i=0; i<sl.size(); i++) {
        String skey = sl.getName(i);
        if (exclude != null && exclude.contains(skey)) continue;
        Object sval = sl.getVal(i);
        int didx = -1;

        if (metrics == true && sval != null && sval instanceof Number) {
            Metric mm = new Metric();
            mm.record(shardAddress, ((Number)sval).longValue());
            sval = mm;
        }
        
        // optimize case where elements are in same position
        if (i < dl.size()) {
          String dkey = dl.getName(i);
          if (skey == dkey || (skey!=null && skey.equals(dkey))) {
            didx = i;
          }
        }

        if (didx == -1) {
          didx = dl.indexOf(skey, 0);
        }

        if (didx == -1) {
          tmp.add(skey, merge(sval, null, null, metrics, shardAddress));
        } else {
          dl.setVal(didx, merge(sval, dl.getVal(didx), null, metrics, shardAddress));
        }
      }
      dl.addAll(tmp);
      return dl;
    }

    // merge unlike elements in a list
    List<Object> t = new ArrayList<>();
    t.add(dest);
    t.add(source);
    return t;
  }


  
  /////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

  @Override
  public String getDescription() {
    return "Debug Information";
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }
}
