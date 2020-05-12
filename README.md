# Helix cluster app

This is a sample cluster app distributing resources and workflow tasks using the Helix framework.  

To run the cluster follow the steps below:
1. Download and run Zookeeper
2. Initialize the cluster running the **ClusterAdmin** class
3. Start the Helix Controller using **ClusterManager** class
4. Add nodes to the cluster running **ClusterNode** as many times as you want
5. Distribute a resource between the nodes calling **CustomResourceManager**
6. Run a workflow to process the resource using **CustomTaskManager**


Useful links:
- https://engineering.linkedin.com/distributed-systems/ad-hoc-task-management-apache-helix
- https://gkishore.wordpress.com/2013/12/02/helix-internals/
- https://github.com/apache/helix/blob/master/helix-core/src/main/java/org/apache/helix/examples/Quickstart.java
- https://www.youtube.com/watch?v=ZJbMDhMq43c
