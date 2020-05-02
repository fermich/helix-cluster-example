package org.apache.helix.rabbitmq;

public class ConsumerThread extends Thread {
  private final String _partition;
  private final String _consumerId;

  public ConsumerThread(String partition, String consumerId) {
    _partition = partition;
    _consumerId = consumerId;
  }

  @Override
  public void run() {
    try {
      while (true) {
        Thread.sleep(10000);
        System.out.println(" work log from consumer: " + _consumerId + " doing job on: " + _partition);
      }
    } catch (InterruptedException e) {
      System.err.println(" [-] " + _consumerId + " on " + _partition + " is interrupted ...");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
