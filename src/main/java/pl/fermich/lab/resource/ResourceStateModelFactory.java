package pl.fermich.lab.resource;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class ResourceStateModelFactory extends StateModelFactory<ResourceStateModel> {
  private final String _consumerId;

  public ResourceStateModelFactory(String consumerId) {
    _consumerId = consumerId;
  }
  //TODO partitions appear here first
  @Override
  public ResourceStateModel createNewStateModel(String resource, String partition) {
    ResourceStateModel model = new ResourceStateModel(_consumerId, partition);
    return model;
  }
}
