package pl.fermich.lab;

import org.apache.helix.task.*;

public class LoadDataTask implements Task {
  private final TaskConfig _taskConfig;
  private final JobConfig _jobConfig;
  private String partition;

  public LoadDataTask(TaskCallbackContext ctx, String partition) {
    _taskConfig = ctx.getTaskConfig();
    _jobConfig = ctx.getJobConfig();
    this.partition = partition;
  }

  public TaskResult run() {
    /* load the data */
    System.out.println("Loading data in LoadDataTask: " + partition);
    return new TaskResult(TaskResult.Status.COMPLETED, null);
  }

  public void cancel() {
    /* Interrupt run() */
    System.out.println("Cancelling LoadDataTask: " + partition);
  }
}
