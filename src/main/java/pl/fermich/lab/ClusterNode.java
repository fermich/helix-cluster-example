package pl.fermich.lab;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import pl.fermich.lab.resource.CustomResourceStateModelFactory;
import pl.fermich.lab.task.CustomTask;
import pl.fermich.lab.task.CustomTaskFactory;

public class ClusterNode {
  private final String zkAddr;
  private final String clusterName;
  private final String nodeId;
  private HelixManager manager = null;

  public ClusterNode(String zkAddr, String clusterName, String nodeId) {
    this.zkAddr = zkAddr;
    this.clusterName = clusterName;
    this.nodeId = nodeId;
  }

  public void connectToCluster() {
    try {
      manager = HelixManagerFactory.getZKHelixManager(clusterName, nodeId, InstanceType.PARTICIPANT, zkAddr);
      StateMachineEngine stateMach = manager.getStateMachineEngine();

      registerCustomResourceFactory(stateMach);
      registerCustomTaskFactory(stateMach);

      manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println("Node " + nodeId + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  private void registerCustomResourceFactory(StateMachineEngine stateMach) {
    CustomResourceStateModelFactory modelFactory = new CustomResourceStateModelFactory(nodeId);
    stateMach.registerStateModelFactory(CustomResourceManager.DEFAULT_STATE_MODEL, modelFactory);
  }

  private void registerCustomTaskFactory(StateMachineEngine stateMach) {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(CustomTask.COMMAND, new CustomTaskFactory());
    stateMach.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME, new TaskStateModelFactory(manager, taskFactoryReg));
  }

  public void disconnect() {
    if (manager != null) {
      manager.disconnect();
    }
  }

  private void registerNodeInstance(ClusterAdmin clusterAdmin, String nodeId) {
    clusterAdmin.runClusterOp(admin -> {
      List<String> nodes = admin.getInstancesInCluster(clusterName);
      if (!nodes.contains(nodeId)) {
        InstanceConfig config = new InstanceConfig(nodeId);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(clusterName, config);
      }
      return null;
    });
  }

  public static void main(String[] args) throws Exception {
//    if (args.length < 3) {
//      System.err
//          .println("USAGE: java ConsumerNode zookeeperAddress (e.g. localhost:2181) consumerId (0-2)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = ClusterInit.DEFAULT_ZK_ADDRESS;
    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;
//    final String nodeId = args[1];
    final String nodeId = "node_0";
    ClusterAdmin clusterAdmin = new ClusterAdmin(zkAddr);

    final ClusterNode clusterNode =
        new ClusterNode(zkAddr, clusterName, nodeId);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down: " + nodeId);
        clusterNode.disconnect();
      }
    });

    clusterNode.registerNodeInstance(clusterAdmin, nodeId);
    clusterNode.connectToCluster();
  }
}
