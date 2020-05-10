package pl.fermich.lab.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

public class CustomTaskFactory implements TaskFactory {
    @Override
    public Task createNewTask(TaskCallbackContext context) {
        String targetPartition = context.getTaskConfig().getTargetPartition();
        return new CustomTask(context, targetPartition);
    }
}
