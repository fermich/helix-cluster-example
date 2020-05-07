package pl.fermich.lab;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import pl.fermich.lab.resource.ResourceStateModelFactory;
import pl.fermich.lab.task.ResourceTask;
import pl.fermich.lab.task.ResourceTaskFactory;

public class ClusterNode {
  private final String _zkAddr;
  private final String _clusterName;
  private final String _consumerId;
  private HelixManager _manager = null;

  public ClusterNode(String zkAddr, String clusterName, String consumerId) {
    _zkAddr = zkAddr;
    _clusterName = clusterName;
    _consumerId = consumerId;
  }

  public void connect() {
    try {
      _manager = HelixManagerFactory.getZKHelixManager(_clusterName, _consumerId, InstanceType.PARTICIPANT, _zkAddr);

      StateMachineEngine stateMach = _manager.getStateMachineEngine();

      ResourceStateModelFactory modelFactory = new ResourceStateModelFactory(_consumerId);
      stateMach.registerStateModelFactory(ClusterInit.DEFAULT_STATE_MODEL, modelFactory);

      //register task factory:
      Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
      taskFactoryReg.put(ResourceTask.COMMAND, new ResourceTaskFactory());
      stateMach.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME, new TaskStateModelFactory(_manager, taskFactoryReg));

      _manager.connect();

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _consumerId + " is interrupted ...");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  public void disconnect() {
    if (_manager != null) {
      _manager.disconnect();
    }
  }

  public static void main(String[] args) throws Exception {
//    if (args.length < 3) {
//      System.err
//          .println("USAGE: java ConsumerNode zookeeperAddress (e.g. localhost:2181) consumerId (0-2)");
//      System.exit(1);
//    }

//    final String zkAddr = args[0];
    final String zkAddr = "localhost:2181";
    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;
//    final String consumerId = args[1];
    final String consumerId = "0";

    ZkClient zkclient = null;
    try {
      // add node to cluster if not already added
      zkclient =
          new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
              ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      List<String> nodes = admin.getInstancesInCluster(clusterName);
      if (!nodes.contains("consumer_" + consumerId)) {
        InstanceConfig config = new InstanceConfig("consumer_" + consumerId);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(clusterName, config);
      }

      // start consumer
      final ClusterNode clusterNode =
          new ClusterNode(zkAddr, clusterName, "consumer_" + consumerId);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down consumer_" + consumerId);
          clusterNode.disconnect();
        }
      });

      clusterNode.connect();
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
  }
}
