package pl.fermich.lab;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;

import java.util.HashMap;
import java.util.Map;

class RegisterTask {
  private String zkConnectString;
  private String clusterName;
  private String instanceName;

  public void register() {
    // Map command to task implementation
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("LoadData", new LoadDataTaskFactory());

    HelixManager helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
            zkConnectString);

    // Register this mapping with Helix
    StateMachineEngine stateMachine = helixManager.getStateMachineEngine();
    stateMachine.registerStateModelFactory("MyTaskId", new TaskStateModelFactory(helixManager, taskFactoryReg));
  }

}

