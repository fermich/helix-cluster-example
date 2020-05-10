package pl.fermich.lab;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;

public class ClusterManager {

  public void startStandaloneController(String zkAddr, String clusterName) {
    try {
      final HelixManager manager = HelixControllerMain.startHelixController(zkAddr, clusterName, null,
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

  public static void main(String[] args) {
    String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;
    String zkAddr = ClusterInit.DEFAULT_ZK_ADDRESS;

    ClusterManager clusterManager = new ClusterManager();
    clusterManager.startStandaloneController(zkAddr, clusterName);
  }
}
