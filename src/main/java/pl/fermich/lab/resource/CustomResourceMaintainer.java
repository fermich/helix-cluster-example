package pl.fermich.lab.resource;

public class CustomResourceMaintainer extends Thread {
  private final String partition;
  private final String nodeId;

  public CustomResourceMaintainer(String partition, String nodeId) {
    this.partition = partition;
    this.nodeId = nodeId;
  }

  @Override
  public void run() {
    try {
      while (true) {
        Thread.sleep(10000);
        System.out.println(" work log from consumer: " + nodeId + " doing job on: " + partition);
      }
    } catch (InterruptedException e) {
      System.err.println(" [-] " + nodeId + " on " + partition + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
