package pl.fermich.lab;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;

import java.util.function.Function;

public class ClusterAdmin {

    private final String zkAddress;

    public ClusterAdmin(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    public void runClusterOp(Function<ZKHelixAdmin, Void> clusterOp) {
        ZkClient zkclient = null;
        try {
            zkclient = new ZkClient(zkAddress, ZkClient.DEFAULT_SESSION_TIMEOUT,
                            ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
            ZKHelixAdmin admin = new ZKHelixAdmin(zkclient);

            clusterOp.apply(admin);

        } finally {
            if (zkclient != null) {
                zkclient.close();
            }
        }
    }
}
