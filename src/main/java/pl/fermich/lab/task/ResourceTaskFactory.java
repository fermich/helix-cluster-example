package pl.fermich.lab.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

public class ResourceTaskFactory implements TaskFactory {
    @Override
    public Task createNewTask(TaskCallbackContext context) {
        String targetPartition = context.getTaskConfig().getTargetPartition();
        return new ResourceTask(context, targetPartition);
    }
}
