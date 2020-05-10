package pl.fermich.lab;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;

public class ClusterInit {
  public static final String DEFAULT_ZK_ADDRESS = "localhost:2181";
  public static final String DEFAULT_CLUSTER_NAME = "helix-cluster-app";

  public static void main(String[] args) {
//    if (args.length < 1) {
//      System.err.println("USAGE: java SetupConsumerCluster zookeeperAddress (e.g. localhost:2181)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = DEFAULT_ZK_ADDRESS;
    final String clusterName = DEFAULT_CLUSTER_NAME;

    ZkClient zkclient = null;
    try {
      zkclient =
          new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      // add cluster
      admin.addCluster(clusterName, true);



      //TODO resource management not necessarily here?
      // add resource "topic" which has 6 partitions
//      String resourceName = ResourceManager.DEFAULT_RESOURCE_NAME;
//      admin.addResource(clusterName, resourceName, ResourceManager.DEFAULT_PARTITION_NUMBER, DEFAULT_STATE_MODEL,
//          RebalanceMode.FULL_AUTO.toString());
//
//      admin.rebalance(clusterName, resourceName, ResourceManager.DEFAULT_REPLICA_NUMBER);

    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }
}
