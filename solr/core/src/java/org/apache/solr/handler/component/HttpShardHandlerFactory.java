package org.apache.solr.handler.component;
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

import org.apache.commons.lang.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class HttpShardHandlerFactory extends ShardHandlerFactory implements org.apache.solr.util.plugin.PluginInfoInitialized {
  protected static Logger log = LoggerFactory.getLogger(HttpShardHandlerFactory.class);
  private static final String DEFAULT_SCHEME = "http";
  
  // We want an executor that doesn't take up any resources if
  // it's not used, so it could be created statically for
  // the distributed search component if desired.
  //
  // Consider CallerRuns policy and a lower max threads to throttle
  // requests at some point (or should we simply return failure?)
  private ThreadPoolExecutor commExecutor = new ThreadPoolExecutor(
      0,
      Integer.MAX_VALUE,
      5, TimeUnit.SECONDS, // terminate idle threads after 5 sec
      new SynchronousQueue<Runnable>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("httpShardExecutor")
  );

  protected HttpClient defaultClient;
  private LBHttpSolrServer loadbalancer;
  //default values:
  int soTimeout = 0; 
  int connectionTimeout = 0; 
  int maxConnectionsPerHost = 20;
  int corePoolSize = 0;
  int maximumPoolSize = Integer.MAX_VALUE;
  int keepAliveTime = 5;
  int queueSize = -1;
  boolean accessPolicy = false;

  private String scheme = null;

  protected interface ReplicaListTransformer {
    public void transform(List<Replica> replicas);
  };
  
  protected class SortingReplicaListTransformer implements ReplicaListTransformer {
    private final Comparator<Replica> replicaComparator;
    public SortingReplicaListTransformer(Comparator<Replica> replicaComparator)
    {
      this.replicaComparator = replicaComparator;      
    }
    public void transform(List<Replica> replicas)
    {
      Collections.sort(replicas, replicaComparator);
    }
  };
  
  protected class ShufflingReplicaListTransformer implements ReplicaListTransformer {
    private final Random r;
    public ShufflingReplicaListTransformer(Random r)
    {
      this.r = r;      
    }
    public void transform(List<Replica> replicas)
    {
      Collections.shuffle(replicas, r);
    }
  };
  
  private final Random r = new Random();

  private final ReplicaListTransformer shufflingReplicaListTransformer = new ShufflingReplicaListTransformer(r);
  
  // URL scheme to be used in distributed search.
  static final String INIT_URL_SCHEME = "urlScheme";

  // The core size of the threadpool servicing requests
  static final String INIT_CORE_POOL_SIZE = "corePoolSize";

  // The maximum size of the threadpool servicing requests
  static final String INIT_MAX_POOL_SIZE = "maximumPoolSize";

  // The amount of time idle threads persist for in the queue, before being killed
  static final String MAX_THREAD_IDLE_TIME = "maxThreadIdleTime";

  // If the threadpool uses a backing queue, what is its maximum size (-1) to use direct handoff
  static final String INIT_SIZE_OF_QUEUE = "sizeOfQueue";

  // Configure if the threadpool favours fairness over throughput
  static final String INIT_FAIRNESS_POLICY = "fairnessPolicy";

  /**
   * Get {@link ShardHandler} that uses the default http client.
   */
  @Override
  public ShardHandler getShardHandler() {
    return getShardHandler(defaultClient);
  }

  /**
   * Get {@link ShardHandler} that uses custom http client.
   */
  public ShardHandler getShardHandler(final HttpClient httpClient){
    return new HttpShardHandler(this, httpClient);
  }

  @Override
  public void init(PluginInfo info) {
    NamedList args = info.initArgs;
    this.soTimeout = getParameter(args, HttpClientUtil.PROP_SO_TIMEOUT, soTimeout);
    this.scheme = getParameter(args, INIT_URL_SCHEME, null);
    if(StringUtils.endsWith(this.scheme, "://")) {
      this.scheme = StringUtils.removeEnd(this.scheme, "://");
    }
    this.connectionTimeout = getParameter(args, HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
    this.maxConnectionsPerHost = getParameter(args, HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    this.corePoolSize = getParameter(args, INIT_CORE_POOL_SIZE, corePoolSize);
    this.maximumPoolSize = getParameter(args, INIT_MAX_POOL_SIZE, maximumPoolSize);
    this.keepAliveTime = getParameter(args, MAX_THREAD_IDLE_TIME, keepAliveTime);
    this.queueSize = getParameter(args, INIT_SIZE_OF_QUEUE, queueSize);
    this.accessPolicy = getParameter(args, INIT_FAIRNESS_POLICY, accessPolicy);
    
    // magic sysprop to make tests reproducible: set by SolrTestCaseJ4.
    String v = System.getProperty("tests.shardhandler.randomSeed");
    if (v != null) {
      r.setSeed(Long.parseLong(v));
    }

    BlockingQueue<Runnable> blockingQueue = (this.queueSize == -1) ?
        new SynchronousQueue<Runnable>(this.accessPolicy) :
        new ArrayBlockingQueue<Runnable>(this.queueSize, this.accessPolicy);

    this.commExecutor = new ThreadPoolExecutor(
        this.corePoolSize,
        this.maximumPoolSize,
        this.keepAliveTime, TimeUnit.SECONDS,
        blockingQueue,
        new DefaultSolrThreadFactory("httpShardExecutor")
    );

    ModifiableSolrParams clientParams = new ModifiableSolrParams();
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
    clientParams.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 10000);
    clientParams.set(HttpClientUtil.PROP_SO_TIMEOUT, soTimeout);
    clientParams.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
    clientParams.set(HttpClientUtil.PROP_USE_RETRY, false);
    this.defaultClient = HttpClientUtil.createClient(clientParams);
    this.loadbalancer = createLoadbalancer(defaultClient);
  }

  protected ThreadPoolExecutor getThreadPoolExecutor(){
    return this.commExecutor;
  }

  protected LBHttpSolrServer createLoadbalancer(HttpClient httpClient){
    return new LBHttpSolrServer(httpClient);
  }

  protected <T> T getParameter(NamedList initArgs, String configKey, T defaultValue) {
    T toReturn = defaultValue;
    if (initArgs != null) {
      T temp = (T) initArgs.get(configKey);
      toReturn = (temp != null) ? temp : defaultValue;
    }
    log.info("Setting {} to: {}", configKey, toReturn);
    return toReturn;
  }


  @Override
  public void close() {
    try {
      ExecutorUtil.shutdownNowAndAwaitTermination(commExecutor);
    } finally {
      try {
        if (defaultClient != null) {
          defaultClient.getConnectionManager().shutdown();
        }
      } finally {
        
        if (loadbalancer != null) {
          loadbalancer.shutdown();
        }
      }
    }
  }

  /**
   * Makes a request to one or more of the given urls, using the configured load balancer.
   *
   * @param req The solr search request that should be sent through the load balancer
   * @param urls The list of solr server urls to load balance across
   * @return The response from the request
   */
  public LBHttpSolrServer.Rsp makeLoadBalancedRequest(final QueryRequest req, List<String> urls)
    throws SolrServerException, IOException {
    return loadbalancer.request(new LBHttpSolrServer.Req(req, urls));
  }

  /**
   * Creates a randomized list of urls for the given shard.
   *
   * @param shard the urls for the shard, separated by '|'
   * @return A list of valid urls (including protocol) that are replicas for the shard
   */
  public List<String> makeURLList(String shard) {
    List<String> urls = StrUtils.splitSmart(shard, "|", true);

    // convert shard to URL
    for (int i=0; i<urls.size(); i++) {
      urls.set(i, buildUrl(urls.get(i)));
    }

    return urls;
  }

  private class ShuffledLiveHostsListReplicaComparator implements Comparator<Replica> {

    private final boolean hostAffinity;
    private final boolean nodeAffinity;
    private final List<String> shuffledLiveHostsList;
    private final List<String> shuffledLiveNodesList;

    private String nodeName_TO_host(String nodeName) {
      // format is host:port_solr
      return nodeName.substring(0, nodeName.indexOf(':'));
    }
    
    ShuffledLiveHostsListReplicaComparator(Set<String> liveNodes, Random r, boolean hostAffinity, boolean nodeAffinity) {
      final Set<String> liveHosts = new HashSet<String>();
      for (String liveNode : liveNodes) {
        liveHosts.add( nodeName_TO_host(liveNode) );        
      }
      this.hostAffinity = hostAffinity;
      this.nodeAffinity = nodeAffinity;
      shuffledLiveHostsList = new ArrayList<String>(liveHosts);
      shuffledLiveNodesList = new ArrayList<String>(liveNodes);
      Collections.shuffle(shuffledLiveHostsList, r);
      Collections.shuffle(shuffledLiveNodesList, r);
    }
    
    @Override
    public int compare(Replica lhs, Replica rhs) {
      int diff = 0;
      if (hostAffinity) {
        diff = (shuffledLiveHostsList.indexOf( nodeName_TO_host(lhs.getNodeName()) ) - shuffledLiveHostsList.indexOf( nodeName_TO_host(rhs.getNodeName()) ));        
      }
      if (0 == diff && nodeAffinity) {
        diff = (shuffledLiveNodesList.indexOf(lhs.getNodeName()) - shuffledLiveNodesList.indexOf(rhs.getNodeName()));
      }
      return diff;
    }
    
  };
  
  ReplicaListTransformer getReplicaListTransformer(final SolrQueryRequest req)
  {
    SolrParams params = req.getParams();
    boolean hostAffinity = false;
    boolean nodeAffinity = false;
    
    String[] replicaAffinities = params.getParams("replicaAffinity");
    if (replicaAffinities != null) {
      for (String replicaAffinity : replicaAffinities) {
        if ("host".equals(replicaAffinity)) {
          hostAffinity = true;
        }
        else if ("node".equals(replicaAffinity)) {
          nodeAffinity = true;
        }
      }
    }
    
    if (hostAffinity || nodeAffinity) {
      Set<String> liveNodes = req.getCore().getCoreDescriptor().getCoreContainer().getZkController().getClusterState().getLiveNodes();
      Comparator<Replica> replicaComparator = new ShuffledLiveHostsListReplicaComparator(liveNodes, r, hostAffinity, nodeAffinity);      
      return new SortingReplicaListTransformer(replicaComparator);
    } else {
      return shufflingReplicaListTransformer;
    }    
  }
  
  /**
   * Creates a new completion service for use by a single set of distributed requests.
   */
  public CompletionService newCompletionService() {
    return new ExecutorCompletionService<ShardResponse>(commExecutor);
  }
  
  /**
   * Rebuilds the URL replacing the URL scheme of the passed URL with the
   * configured scheme replacement.If no scheme was configured, the passed URL's
   * scheme is left alone.
   */
  private String buildUrl(String url) {
    if(!URLUtil.hasScheme(url)) {
      return StringUtils.defaultIfEmpty(scheme, DEFAULT_SCHEME) + "://" + url;
    } else if(StringUtils.isNotEmpty(scheme)) {
      return scheme + "://" + URLUtil.removeScheme(url);
    }
    
    return url;
  }
}
