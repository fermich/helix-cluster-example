package pl.fermich.lab;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.*;
import pl.fermich.lab.task.ResourceTask;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TaskManager {
  private final String zkAddr;
  private final String clusterName;
  private final String nodeId;
  private HelixManager manager = null;

  public TaskManager(String zkAddr, String clusterName, String nodeId) {
    this.zkAddr = zkAddr;
    this.clusterName = clusterName;
    this.nodeId = nodeId;
  }

  public void connect() {
    try {
      manager = HelixManagerFactory.getZKHelixManager(clusterName, nodeId, InstanceType.CONTROLLER, zkAddr);
      manager.connect();

      TaskDriver taskDriver = new TaskDriver(manager);
      Workflow myWorkflow = configureWorkflow("Workflow3", "Job3");

      taskDriver.delete("Workflow3");
      taskDriver.start(myWorkflow);
     // taskDriver.stop("Workflow3");

      //taskDriver.stop(myWorkflow);
      //taskDriver.resume(myWorkflow);
      //taskDriver.delete(myWorkflow);

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println(" [-] " + nodeId + " is interrupted ...");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  private Workflow configureWorkflow(String workflowName, String jobName) {
    Workflow.Builder myWorkflowBuilder = new Workflow.Builder(workflowName);
    myWorkflowBuilder.setExpiry(200000L)
            .setScheduleConfig(ScheduleConfig.recurringFromNow(TimeUnit.MINUTES, 2));

    myWorkflowBuilder.addJob(jobName, configureJob());

    //myWorkflowBuilder.addParentChildDependency(ParentJobName, ChildJobName);

    Workflow myWorkflow = myWorkflowBuilder.build();
    return myWorkflow;
  }

  private JobConfig.Builder configureJob() {
    //TODO command
//    TaskConfig taskCfg = new TaskConfig(null, null, null, null);
//    List<TaskConfig> taskCfgs = new ArrayList<TaskConfig>();
//    taskCfgs.add(taskCfg);

    JobConfig.Builder myJobCfgBuilder = new JobConfig.Builder();

//    # Rather than defining individual tasks, start a task on each MASTER replica of MyDB partitions
//      targetResource: MyDB
//      targetPartitionStates: [MASTER]

    //start a task on each MASTER replica of target resource partitions
    myJobCfgBuilder.setCommand(ResourceTask.COMMAND).setNumberOfTasks(2);
    myJobCfgBuilder.setTargetResource(ResourceManager.DEFAULT_RESOURCE_NAME);
    //myJobCfgBuilder.setTargetPartitions()
    //myJobCfgBuilder.setTargetPartitionStates(Sets.newHashSet("MASTER"));
    //myJobCfgBuilder.addTaskConfigs(taskCfgs);
    return myJobCfgBuilder;
  }

  public void disconnect() {
    if (manager != null) {
      manager.disconnect();
    }
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
//    final String consumerId = args[1];
    final String consumerId = "1";

    ZkClient zkclient = null;
    try {
      // add node to cluster if not already added
      zkclient =
              new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
                      ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
      ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

      List<String> nodes = admin.getInstancesInCluster(clusterName);
      if (!nodes.contains("task_" + consumerId)) {
        InstanceConfig config = new InstanceConfig("task_" + consumerId);
        config.setHostName("localhost");
        config.setInstanceEnabled(true);
        admin.addInstance(clusterName, config);
      }

      final TaskManager taskManager =
              new TaskManager(zkAddr, clusterName, "task_" + consumerId);

      // start consumer
//      final ConsumerNode consumerNode =
//              new ConsumerNode(zkAddr, clusterName, "consumer_" + consumerId);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutting down task_" + consumerId);
          taskManager.disconnect();
        }
      });

      taskManager.connect();
    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }


//    if (args.length < 3) {
//      System.err
//          .println("USAGE: java ConsumerNode zookeeperAddress (e.g. localhost:2181) taskId (0-2)");
//      System.exit(1);
//    }

  }
}
