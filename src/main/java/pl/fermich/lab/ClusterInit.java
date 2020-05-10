package pl.fermich.lab;

public class ClusterInit {
  public static final String DEFAULT_ZK_ADDRESS = "localhost:2181";
  public static final String DEFAULT_CLUSTER_NAME = "helix-cluster-app";

  public static void main(String[] args) {
    String zkAddr = DEFAULT_ZK_ADDRESS;
    String clusterName = DEFAULT_CLUSTER_NAME;

    ClusterAdmin clusterAdmin = new ClusterAdmin(zkAddr);

    clusterAdmin.runClusterOp(admin -> {
      admin.addCluster(clusterName, true);
      return null;
    });
  }
}
