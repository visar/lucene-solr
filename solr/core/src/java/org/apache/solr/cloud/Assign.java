package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET_SHUFFLE_DEFAULT;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;


public class Assign {
  private static Pattern COUNT = Pattern.compile("core_node(\\d+)");
  private static Logger log = LoggerFactory
      .getLogger(Assign.class);

  public static String assignNode(String collection, ClusterState state) {
    Map<String, Slice> sliceMap = state.getSlicesMap(collection);
    if (sliceMap == null) {
      return "core_node1";
    }

    int max = 0;
    for (Slice slice : sliceMap.values()) {
      for (Replica replica : slice.getReplicas()) {
        Matcher m = COUNT.matcher(replica.getName());
        if (m.matches()) {
          max = Math.max(max, Integer.parseInt(m.group(1)));
        }
      }
    }

    return "core_node" + (max + 1);
  }
  
  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   * 
   * @return the assigned shard id
   */
  public static String assignShard(String collection, ClusterState state, Integer numShards) {
    if (numShards == null) {
      numShards = 1;
    }
    String returnShardId = null;
    Map<String, Slice> sliceMap = state.getActiveSlicesMap(collection);


    // TODO: now that we create shards ahead of time, is this code needed?  Esp since hash ranges aren't assigned when creating via this method?

    if (sliceMap == null) {
      return "shard1";
    }

    List<String> shardIdNames = new ArrayList<>(sliceMap.keySet());

    if (shardIdNames.size() < numShards) {
      return "shard" + (shardIdNames.size() + 1);
    }

    // TODO: don't need to sort to find shard with fewest replicas!

    // else figure out which shard needs more replicas
    final Map<String, Integer> map = new HashMap<>();
    for (String shardId : shardIdNames) {
      int cnt = sliceMap.get(shardId).getReplicasMap().size();
      map.put(shardId, cnt);
    }

    Collections.sort(shardIdNames, new Comparator<String>() {

      @Override
      public int compare(String o1, String o2) {
        Integer one = map.get(o1);
        Integer two = map.get(o2);
        return one.compareTo(two);
      }
    });

    returnShardId = shardIdNames.get(0);
    return returnShardId;
  }

  static   class Node {
    public  final String nodeName;
    public int thisCollectionNodes=0;
    public int totalNodes=0;

    Node(String nodeName) {
      this.nodeName = nodeName;
    }

    public int weight(){
      return (thisCollectionNodes * 100) + totalNodes;
    }
  }

  public static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random) {
    // TODO: add smarter options that look at the current number of cores per
    // node?
    // for now we just go random (except when createNodeSet and createNodeSet.shuffle=false are passed in)

    List<String> nodeList;

    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null)?null:StrUtils.splitSmart(createNodeSetStr, ",", true);

    if (createNodeList != null) {
      nodeList = new ArrayList<>(createNodeList);
      nodeList.retainAll(liveNodes);
      if (message.getBool(CREATE_NODE_SET_SHUFFLE, CREATE_NODE_SET_SHUFFLE_DEFAULT)) {
        Collections.shuffle(nodeList, random);
      }
    } else {
      nodeList = new ArrayList<>(liveNodes);
      Collections.shuffle(nodeList, random);
    }
    
    return nodeList;    
  }
  
  public static ArrayList<Node> getNodesForNewShard(ClusterState clusterState, ZkNodeProps message, String collectionName, int numSlices, int maxShardsPerNode, int repFactor, final String action_description, final Random random) {
    final List<String> nodeList = getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, random);

    HashMap<String,Node> nodeNameVsShardCount =  new HashMap<>();
    for (String s : nodeList) nodeNameVsShardCount.put(s,new Node(s));
    for (String s : clusterState.getCollections()) {
      DocCollection c = clusterState.getCollection(s);
      //identify suitable nodes  by checking the no:of cores in each of them
      for (Slice slice : c.getSlices()) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          Node count = nodeNameVsShardCount.get(replica.getNodeName());
          if (count != null) {
            count.totalNodes++;
            if (s.equals(collectionName)) {
              count.thisCollectionNodes++;
              if (count.thisCollectionNodes >= maxShardsPerNode) nodeNameVsShardCount.remove(replica.getNodeName());
            }
          }
        }
      }
    }

    if (nodeNameVsShardCount.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot "+action_description
          + ". No suitable Solr-instances among those currently live or live and part of your " + CREATE_NODE_SET + "(" + nodeList +")");
    }

    if (repFactor > nodeNameVsShardCount.size()) {
      log.warn("Specified "
          + ZkStateReader.REPLICATION_FACTOR
          + " of "
          + repFactor
          + " on collection "
          + collectionName
          + " is higher than or equal to the number of suitable Solr instances currently live or live and part of your " + CREATE_NODE_SET + "("
          + nodeNameVsShardCount.size()
          + "). Its unusual to run two replica of the same slice on the same Solr-instance.");
    }

    int maxCoresAllowedToCreate = maxShardsPerNode * nodeNameVsShardCount.size();
    int requestedCoresToCreate = numSlices * repFactor;
    int minCoresToCreate = requestedCoresToCreate;
    if (maxCoresAllowedToCreate < minCoresToCreate) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Cannot create shards " + collectionName + ". Value of "
          + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
          + ", and the number of suitable nodes currently live or part and your "+CREATE_NODE_SET+" is " + nodeNameVsShardCount.size()
          + ". This allows a maximum of " + maxCoresAllowedToCreate
          + " to be created. Value of " + NUM_SLICES + " is " + numSlices
          + " and value of " + ZkStateReader.REPLICATION_FACTOR + " is " + repFactor
          + ". This requires " + requestedCoresToCreate
          + " shards to be created (higher than the allowed number)");
    }

    ArrayList<Node> sortedNodeList = new ArrayList<>(nodeNameVsShardCount.values());
    Collections.sort(sortedNodeList, new Comparator<Node>() {
      @Override
      public int compare(Node x, Node y) {
        return (x.weight() < y.weight()) ? -1 : ((x.weight() == y.weight()) ? 0 : 1);
      }
    });
    return sortedNodeList;
  }

}
