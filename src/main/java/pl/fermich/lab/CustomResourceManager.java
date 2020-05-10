package pl.fermich.lab;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

public class CustomResourceManager {

  public static final String DEFAULT_STATE_MODEL = "OnlineOffline";
  public static final String DEFAULT_RESOURCE_NAME = "custom-resource";
  public static final int DEFAULT_PARTITION_NUMBER = 6;
  public static final int DEFAULT_REPLICA_NUMBER = 1;

  public static void main(String[] args) {
//    if (args.length < 1) {
//      System.err.println("USAGE: java SetupConsumerCluster zookeeperAddress (e.g. localhost:2181)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = ClusterInit.DEFAULT_ZK_ADDRESS;
    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;

    ClusterAdmin clusterAdmin = new ClusterAdmin(zkAddr);

    clusterAdmin.runClusterOp(admin -> {
      addResource(admin, clusterName, DEFAULT_RESOURCE_NAME, DEFAULT_PARTITION_NUMBER);
//      deleteResource(admin, clusterName, DEFAULT_RESOURCE_NAME);
//      addResource(admin, clusterName, "mycustomresource", 3);
//      deleteResource(admin, clusterName, "mycustomresource");
      return null;
    });


  }

  private static void addResource(ZKHelixAdmin admin, String clusterName, String resourceName, int partitions) {
    // add state model definition
    admin.addStateModelDef(clusterName, DEFAULT_STATE_MODEL,
            new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));

    admin.addResource(clusterName, resourceName, partitions, DEFAULT_STATE_MODEL,
            RebalanceMode.FULL_AUTO.toString());
    admin.rebalance(clusterName, resourceName, DEFAULT_REPLICA_NUMBER);
  }

  private static void deleteResource(ZKHelixAdmin admin, String clusterName, String resourceName) {
    admin.dropResource(clusterName, resourceName);
  }
}
