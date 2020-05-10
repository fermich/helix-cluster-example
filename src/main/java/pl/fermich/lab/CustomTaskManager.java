package pl.fermich.lab;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.task.*;
import pl.fermich.lab.task.CustomTask;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class CustomTaskManager {
  private final String zkAddr;
  private final String clusterName;
  private final String taskId;
  private HelixManager manager = null;

  public CustomTaskManager(String zkAddr, String clusterName, String taskId) {
    this.zkAddr = zkAddr;
    this.clusterName = clusterName;
    this.taskId = taskId;
  }

  public void startWorkflow(String workflowName, String jobName) {
    try {
      manager = HelixManagerFactory.getZKHelixManager(clusterName, taskId, InstanceType.CONTROLLER, zkAddr);
      manager.connect();

      TaskDriver taskDriver = new TaskDriver(manager);
      Workflow workflow = configureWorkflow(workflowName, jobName, 2);

      taskDriver.delete(workflowName);
      taskDriver.start(workflow);
//      taskDriver.stop(workflowName);
//      taskDriver.resume(workflowName);

      Thread.currentThread().join();
    } catch (InterruptedException e) {
      System.err.println("Task " + taskId + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      disconnect();
    }
  }

  private Workflow configureWorkflow(String workflowName, String jobName, int numberOfTasks) {
    Workflow.Builder myWorkflowBuilder = new Workflow.Builder(workflowName);
    myWorkflowBuilder.setExpiry(200000L)
            .setScheduleConfig(ScheduleConfig.recurringFromNow(TimeUnit.MINUTES, 2));

    myWorkflowBuilder.addJob(jobName, configureJob(numberOfTasks));
    //TODO: consider parenting
    //myWorkflowBuilder.addParentChildDependency(ParentJobName, ChildJobName);

    Workflow myWorkflow = myWorkflowBuilder.build();
    return myWorkflow;
  }

  private JobConfig.Builder configureJob(int numberOfTasks) {
    JobConfig.Builder myJobCfgBuilder = new JobConfig.Builder();
    myJobCfgBuilder.setCommand(CustomTask.COMMAND).setNumberOfTasks(numberOfTasks);
    myJobCfgBuilder.setTargetResource(CustomResourceManager.DEFAULT_RESOURCE_NAME);
    //myJobCfgBuilder.setTargetPartitions()
    //TODO: it is possible to start a task on each MASTER replica of the resource partitions:
    //myJobCfgBuilder.setTargetPartitionStates(Sets.newHashSet("MASTER"));
    //myJobCfgBuilder.addTaskConfigs(taskCfgs);
    return myJobCfgBuilder;
  }

  public void disconnect() {
    if (manager != null) {
      manager.disconnect();
    }
  }

  private void registerTaskInstance(ClusterAdmin clusterAdmin) {
    clusterAdmin.runClusterOp(admin -> {
      List<String> nodes = admin.getInstancesInCluster(clusterName);
      if (!nodes.contains(taskId)) {
        InstanceConfig config = new InstanceConfig(taskId);
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

    ClusterAdmin clusterAdmin = new ClusterAdmin(ClusterInit.DEFAULT_ZK_ADDRESS);

    final String clusterName = ClusterInit.DEFAULT_CLUSTER_NAME;
//    final String taskId = args[1];
    final String taskId = "task_1";

    final CustomTaskManager taskManager = new CustomTaskManager(zkAddr, clusterName, taskId);

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.out.println("Shutting down: " + taskId);
        taskManager.disconnect();
      }
    });

    taskManager.registerTaskInstance(clusterAdmin);
    taskManager.startWorkflow("CustomWorkflow", "CustomJob");
  }
}
