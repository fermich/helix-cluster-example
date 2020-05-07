package pl.fermich.lab.task;

import org.apache.helix.task.*;

public class LoadDataTask implements Task {
  private final TaskConfig _taskConfig;
  private final JobConfig _jobConfig;
  private String partition;
  public static final String COMMAND = "LoadData";

  public LoadDataTask(TaskCallbackContext ctx, String partition) {
    _taskConfig = ctx.getTaskConfig();
    _jobConfig = ctx.getJobConfig();
    this.partition = partition;
  }

  public TaskResult run() {
    /* load the data */
    System.out.println("[LoadDataTask] Processing: " + partition);
    return new TaskResult(TaskResult.Status.COMPLETED, null);
  }

  public void cancel() {
    /* Interrupt run() */
    System.out.println("[LoadDataTask] Cancelling: " + partition);
  }
}
