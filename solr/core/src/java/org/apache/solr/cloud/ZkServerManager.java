package org.apache.solr.cloud;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.LoggerFactory;

public class ZkServerManager extends SolrZkServer {
    static org.slf4j.Logger log = LoggerFactory.getLogger(SolrZkServer.class);

    public ZkServerManager(String zkRun, String zkHost, String dataHome,
            String confHome, String solrPort) {
        super(zkRun, zkHost, dataHome, confHome, solrPort);
    }

    public SolrZkServer zkServer = null;

    public static void main(String[] args) {
        try {
            String zkConfHome = System.getProperty("zkServerConfDir");
            if (null == zkConfHome || zkConfHome.isEmpty()) {
                log.error("config file name is missing. Please set zkServerConfDir system property");
                return;
            }

            String zkDataHome = System.getProperty("zkServerDataDir");

            String zookeeperHost = System.getProperty("zkHost");// this is the
                                                                // list of all
                                                                // the ZKs

            String hostPort = "";
            String zookeeperPort = System.getProperty("zkPort");
            if (null != zookeeperPort) {
                hostPort = String.valueOf(Integer.parseInt(zookeeperPort) - 1000);
            }

            // This should be the hostname:port of the "local" zookeeper instance to run.
            // It should match with one of the entries in zkHost (zookeeperHost)
            String zkRun = System.getProperty("zkRun");
            if (null == zkRun || zkRun.isEmpty()) {
                log.error("zk host:port info is missing. Please set zkRun system property");
                return;
            }

            // zookeeper in quorum mode currently causes a failure when trying
            // to register log4j mbeans. See SOLR-2369
            // TODO: remove after updating to an slf4j based zookeeper
            System.setProperty("zookeeper.jmx.log4j.disable", "true");

            ZkServerManager serverMgr = new ZkServerManager(zkRun,
                    zookeeperHost, zkDataHome, zkConfHome, hostPort);
            serverMgr.parseConfig();
            serverMgr.start();
            try {
                while (true) {
                    Thread.sleep(60000);
                }
            } catch (InterruptedException ex) {
                ;
                ;
            }
            serverMgr.stop();
        } catch (Exception ex) {
            log.error(ExceptionUtils.getStackTrace(ex));
        }
    }

}
