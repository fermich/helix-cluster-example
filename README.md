https://engineering.linkedin.com/distributed-systems/ad-hoc-task-management-apache-helix


https://gkishore.wordpress.com/2013/12/02/helix-internals/


https://github.com/apache/helix/blob/master/helix-core/src/main/java/org/apache/helix/examples/Quickstart.java

https://www.youtube.com/watch?v=ZJbMDhMq43c

The work of both Task Framework and the generic resource management framework are managed by a single, central scheduler: Helix Controller.
That means that a single Controller runs in one JVM with no isolation, and we cannot prevent a slowdown in resource management from affecting 
assigning and scheduling of tasks, and vice versa. In other words, there is a resource competition between the two entities. 
In this sense, we need to separate one Helix Controller into two separate Controllers running independently—a Helix Controller and a Task Framework Controller.

