package org.apache.helix.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;

@StateModelInfo(initialState = "OFFLINE", states = {
    "ONLINE", "ERROR"
})
public class ConsumerStateModel extends StateModel {
  private static Logger LOG = LoggerFactory.getLogger(ConsumerStateModel.class);

  private final String _consumerId;
  private final String _partition;

  private ConsumerThread _thread = null;

  public ConsumerStateModel(String consumerId, String partition) {
    _partition = partition;
    _consumerId = consumerId;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes ONLINE from OFFLINE for " + _partition);

    if (_thread == null) {
      LOG.debug("Starting ConsumerThread for " + _partition + "...");
      _thread = new ConsumerThread(_partition, _consumerId);
      _thread.start();
      LOG.debug("Starting ConsumerThread for " + _partition + " done");

    }
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.debug(_consumerId + " becomes OFFLINE from ONLINE for " + _partition);

    if (_thread != null) {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      _thread.join(2000);
      _thread = null;
      LOG.debug("Stopping " + _consumerId + " for " + _partition + " done");

    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes DROPPED from OFFLINE for " + _partition);
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    LOG.debug(_consumerId + " becomes OFFLINE from ERROR for " + _partition);
  }

  @Override
  public void reset() {
    LOG.warn("Default reset() invoked");

    if (_thread != null) {
      LOG.debug("Stopping " + _consumerId + " for " + _partition + "...");

      _thread.interrupt();
      try {
        _thread.join(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      _thread = null;
      LOG.debug("Stopping " + _consumerId + " for " + _partition + " done");

    }
  }
}
