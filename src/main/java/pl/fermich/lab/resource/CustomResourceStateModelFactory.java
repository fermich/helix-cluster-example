package pl.fermich.lab.resource;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class CustomResourceStateModelFactory extends StateModelFactory<CustomResourceStateModel> {
  private final String nodeId;

  public CustomResourceStateModelFactory(String nodeId) {
    this.nodeId = nodeId;
  }
  //TODO partitions appear here first
  @Override
  public CustomResourceStateModel createNewStateModel(String resource, String partition) {
    CustomResourceStateModel model = new CustomResourceStateModel(nodeId, partition);
    return model;
  }
}
