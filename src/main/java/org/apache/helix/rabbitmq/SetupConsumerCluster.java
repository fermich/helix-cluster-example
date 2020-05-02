package org.apache.helix.rabbitmq;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

public class SetupConsumerCluster {
  public static final String DEFAULT_CLUSTER_NAME = "rabbitmq-consumer-cluster";
  public static final String DEFAULT_RESOURCE_NAME = "topic";
  public static final int DEFAULT_PARTITION_NUMBER = 6;
  public static final String DEFAULT_STATE_MODEL = "OnlineOffline";

  public static void main(String[] args) {
//    if (args.length < 1) {
//      System.err.println("USAGE: java SetupConsumerCluster zookeeperAddress (e.g. localhost:2181)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = "localhost:2181";
    final String clusterName = DEFAULT_CLUSTER_NAME;

    ZkClient zkclient = null;
    try {
      zkclient =
          new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      // add cluster
      admin.addCluster(clusterName, true);

      // add state model definition
      admin.addStateModelDef(clusterName, DEFAULT_STATE_MODEL,
          new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline()));

      //TODO resource management not necessarily here?
      // add resource "topic" which has 6 partitions
      String resourceName = DEFAULT_RESOURCE_NAME;
      admin.addResource(clusterName, resourceName, DEFAULT_PARTITION_NUMBER, DEFAULT_STATE_MODEL,
          RebalanceMode.FULL_AUTO.toString());

      admin.rebalance(clusterName, resourceName, 1);

    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }
}
