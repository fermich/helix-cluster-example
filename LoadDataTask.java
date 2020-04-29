
public class LoadDataTask implements Task {
  private final TaskConfig _taskConfig;
  private final JobConfig _jobConfig;

  public LoadDataTask(TaskCallbackContext ctx) {
    _taskConfig = ctx.getTaskConfig();
    _jobConfig = ctx.getJobConfig();
  }

  public TaskResult run() {
    /* load the data */
    return new TaskResult(TaskResult.Status.COMPLETED, null);
  }

  public void cancel() {
    /* Interrupt run() */
  }
}
