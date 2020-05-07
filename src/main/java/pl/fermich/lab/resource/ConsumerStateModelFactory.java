package pl.fermich.lab.resource;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class ConsumerStateModelFactory extends StateModelFactory<ConsumerStateModel> {
  private final String _consumerId;

  public ConsumerStateModelFactory(String consumerId) {
    _consumerId = consumerId;
  }
  //TODO partitions appear here first
  @Override
  public ConsumerStateModel createNewStateModel(String resource, String partition) {
    ConsumerStateModel model = new ConsumerStateModel(_consumerId, partition);
    return model;
  }
}
