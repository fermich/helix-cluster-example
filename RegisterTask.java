
// Map command to task implementation
Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
taskFactoryReg.put("LoadData", new TaskFactory() {
  @Override
  public Task createNewTask(TaskCallbackContext context) {
    return new LoadDataTask(context);
  }
});
 
// Register this mapping with Helix
StateMachineEngine stateMachine = helixManager.getStateMachineEngine();
stateMachine.registerStateModelFactory(StateModelDefId.from("Task"),
    new TaskStateModelFactory(helixManager, taskFactoryReg));
