package org.apache.solr.handler;

import java.util.ArrayList;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.PingRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterPingRequestHandler extends PingRequestHandler
{
    public static Logger log = LoggerFactory.getLogger(ClusterPingRequestHandler.class);

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
            throws Exception
    {
        // Run a normal ping request first.
        super.handleRequestBody(req, rsp);

        // If the normal ping worked (no exception), try our cluster state checks
        if (rsp.getException() == null)
        {
            // No null checks in here, if anything fails, e.g. req.getCore() is null,
            // or Slice or Replica are null, then we just set the exception in the
            // response and let the client detect that.
            try
            {
                CoreContainer coreContainer = req.getCore().getCoreDescriptor().getCoreContainer();

                // If we have a ZK core
                if (coreContainer.isZooKeeperAware())
                {
                    CloudDescriptor cloudDescriptor = req.getCore().getCoreDescriptor().getCloudDescriptor();

                    ZkController zkController = coreContainer.getZkController();
                    String zkNodeName = zkController.getNodeName();
                    ClusterState zkClusterState = zkController.getClusterState();

                    String collection = cloudDescriptor.getCollectionName();
                    String shardId = cloudDescriptor.getShardId();
                    Slice slice = zkClusterState.getSlice(collection, shardId);

                    String coreNodeName = cloudDescriptor.getCoreNodeName();
                    Replica replica = slice.getReplicasMap().get(coreNodeName);

                    String state = replica.getStr(ZkStateReader.STATE_PROP);
                    String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);

                    boolean leader = replica.equals(zkClusterState.getLeader(collection, shardId));
                    boolean live = zkClusterState.liveNodesContain(zkNodeName);

                    rsp.add("collection", collection);
                    rsp.add("shard", shardId); // slice.getName() is another way to get this(!)
                    rsp.add("coreName", req.getCore().getName()); // this is the local core name, e.g. collection1_shard1_replica1
                    rsp.add("replicaState", state); // this is "state" in ZK replica info, *not* shard-level state
                    rsp.add("replicaName", coreNodeName); // this is the key/name in ZK replica list, e.g. host1:8787_solr_collection1_shard1_replica1
                    rsp.add("nodeName", zkNodeName); // this is replica.getNodeName() and "node_name" in ZK replica, e.g. host1:8787_solr
                    rsp.add("baseUrl", baseUrl); // "base_url" in ZK replica info, e.g. http://host1:8787/solr
                    rsp.add("leader", leader); // "leader" in ZK replica info
                    rsp.add("live", live);

                    // tell which other nodes have live replicas of this slice
                    ArrayList<String> live_slice_replica_url = new ArrayList<String>();
                    for (Replica rr : slice.getReplicas()) {
                      String rr_zkNodeName = rr.getNodeName();
                      if (!zkNodeName.equals(rr_zkNodeName) && zkClusterState.liveNodesContain(rr_zkNodeName)) {
                        live_slice_replica_url.add(rr.getStr(ZkStateReader.BASE_URL_PROP)+"/"+rr.getStr(ZkStateReader.CORE_NAME_PROP));
                      }
                    }
                    rsp.add("liveReplicaPeers", live_slice_replica_url);
                }
                // No else, if not ZK aware, then we just return a normal Ping response.
            } catch (Exception e) {
                log.error("ERROR executing ClusterPingRequest:", e);

                rsp.setException(e);
            }
        }
    }

    // ////////////////////// SolrInfoMBeans methods //////////////////////

    @Override
    public String getDescription()
    {
        return "Reports application health including cluster state to a load-balancer";
    }

    @Override
    public String getSource()
    {
        return "$URL$";
    }
}
