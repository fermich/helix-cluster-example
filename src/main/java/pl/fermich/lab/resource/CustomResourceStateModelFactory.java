package pl.fermich.lab.resource;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class CustomResourceStateModelFactory extends StateModelFactory<CustomResourceStateModel> {
  private final String nodeId;

  public CustomResourceStateModelFactory(String nodeId) {
    this.nodeId = nodeId;
  }

  @Override
  public CustomResourceStateModel createNewStateModel(String resource, String partition) {
    //partition number appears here first
    return new CustomResourceStateModel(nodeId, partition);
  }
}
