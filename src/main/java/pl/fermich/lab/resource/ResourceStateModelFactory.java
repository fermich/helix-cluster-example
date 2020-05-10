package pl.fermich.lab.resource;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class ResourceStateModelFactory extends StateModelFactory<ResourceStateModel> {
  private final String nodeId;

  public ResourceStateModelFactory(String nodeId) {
    this.nodeId = nodeId;
  }
  //TODO partitions appear here first
  @Override
  public ResourceStateModel createNewStateModel(String resource, String partition) {
    ResourceStateModel model = new ResourceStateModel(nodeId, partition);
    return model;
  }
}
