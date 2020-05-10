package pl.fermich.lab;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;

public class ClusterManager {
  public static void main(String[] args) {
//    if (args.length < 1) {
//      System.err.println("USAGE: java StartClusterManager zookeeperAddress (e.g. localhost:2181)");
//      System.exit(1);
//    }

    //    final String zkAddr = args[0];
    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;
    final String zkAddr = ClusterInit.DEFAULT_ZK_ADDRESS;

    try {
      final HelixManager manager =
          HelixControllerMain.startHelixController(zkAddr, clusterName, null,
              HelixControllerMain.STANDALONE);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down cluster manager: " + manager.getInstanceName());
          manager.disconnect();
        }
      });

      Thread.currentThread().join();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
