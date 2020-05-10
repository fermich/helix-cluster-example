package pl.fermich.lab.task;

import org.apache.helix.task.*;

public class ResourceTask implements Task {
  private final TaskConfig taskConfig;
  private final JobConfig jobConfig;
  private String partition;
  public static final String COMMAND = "LoadData";

  public ResourceTask(TaskCallbackContext ctx, String partition) {
    taskConfig = ctx.getTaskConfig();
    jobConfig = ctx.getJobConfig();
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
