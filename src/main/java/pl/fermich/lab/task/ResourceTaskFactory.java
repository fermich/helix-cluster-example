package pl.fermich.lab.task;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;

public class ResourceTaskFactory implements TaskFactory {
    @Override
    public Task createNewTask(TaskCallbackContext context) {
        /**
         * Returns a {@link Task} instance.
         * @param context Contextual information for the task, including task and job configurations
         * @return A {@link Task} instance.
         */
        String targetPartition = context.getTaskConfig().getTargetPartition();
        return new ResourceTask(context, targetPartition);
    }
}
