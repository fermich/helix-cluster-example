package pl.fermich.lab.resource;

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
public class CustomResourceStateModel extends StateModel {
  private static Logger LOG = LoggerFactory.getLogger(CustomResourceStateModel.class);

  private final String nodeId;
  private final String partition;

  private CustomResourceMaintainer maintainer = null;

  public CustomResourceStateModel(String nodeId, String partition) {
    this.nodeId = nodeId;
    this.partition = partition;
  }

  @Transition(to = "ONLINE", from = "OFFLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    LOG.debug(nodeId + " becomes ONLINE from OFFLINE for " + partition);

    if (maintainer == null) {
      LOG.debug("Starting ConsumerThread for " + partition + "...");
      maintainer = new CustomResourceMaintainer(partition, nodeId);
      maintainer.start();
      LOG.debug("Starting ConsumerThread for " + partition + " done");

    }
  }

  @Transition(to = "OFFLINE", from = "ONLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context)
      throws InterruptedException {
    LOG.debug(nodeId + " becomes OFFLINE from ONLINE for " + partition);

    if (maintainer != null) {
      LOG.debug("Stopping " + nodeId + " for " + partition + "...");

      maintainer.interrupt();
      maintainer.join(2000);
      maintainer = null;
      LOG.debug("Stopping " + nodeId + " for " + partition + " done");

    }
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    LOG.debug(nodeId + " becomes DROPPED from OFFLINE for " + partition);
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context) {
    LOG.debug(nodeId + " becomes OFFLINE from ERROR for " + partition);
  }

  @Override
  public void reset() {
    LOG.warn("Default reset() invoked");

    if (maintainer != null) {
      LOG.debug("Stopping " + nodeId + " for " + partition + "...");

      maintainer.interrupt();
      try {
        maintainer.join(2000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      maintainer = null;
      LOG.debug("Stopping " + nodeId + " for " + partition + " done");

    }
  }
}
