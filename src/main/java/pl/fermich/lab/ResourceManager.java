package pl.fermich.lab;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.RebalanceMode;

public class ResourceManager {

  public static final String DEFAULT_RESOURCE_NAME = "topic";
  public static final int DEFAULT_PARTITION_NUMBER = 6;

  public static void main(String[] args) {
//    if (args.length < 1) {
//      System.err.println("USAGE: java SetupConsumerCluster zookeeperAddress (e.g. localhost:2181)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = ClusterInit.DEFAULT_ZK_ADDRESS;
    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;

    ZkClient zkclient = null;
    try {
      zkclient =
          new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      addResource(admin, clusterName, DEFAULT_RESOURCE_NAME, DEFAULT_PARTITION_NUMBER);
//      addResource(admin, clusterName, "mycustomresource", 3);
//      deleteResource(admin, clusterName, DEFAULT_RESOURCE_NAME);
      //deleteResource(admin, clusterName, "mycustomresource");
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }

  private static void addResource(ZKHelixAdmin admin, String clusterName, String resourceName, int partitions) {
    admin.addResource(clusterName, resourceName, partitions, ClusterInit.DEFAULT_STATE_MODEL,
            RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, 1);
  }

  private static void deleteResource(ZKHelixAdmin admin, String clusterName, String resourceName) {
    admin.dropResource(clusterName, resourceName);
    //TODO need to call rebalance()?
    //admin.rebalance(clusterName, resourceName, 1);
  }
}
