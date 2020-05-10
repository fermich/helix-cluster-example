package pl.fermich.lab.task;

import org.apache.helix.task.*;

public class CustomTask implements Task {
  private final TaskConfig taskConfig;
  private final JobConfig jobConfig;
  private String partition;
  public static final String COMMAND = "LoadData";

  public CustomTask(TaskCallbackContext ctx, String partition) {
    taskConfig = ctx.getTaskConfig();
    jobConfig = ctx.getJobConfig();
    this.partition = partition;
  }

  public TaskResult run() {
    System.out.println("Starting task for custom resource partition: " + partition);
    return new TaskResult(TaskResult.Status.COMPLETED, null);
  }

  public void cancel() {
    /* Interrupt run() */
    System.out.println("Cancelling task for partition: " + partition);
  }
}
