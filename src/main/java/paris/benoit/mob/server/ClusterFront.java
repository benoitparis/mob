package paris.benoit.mob.server;

public interface ClusterFront {

    void start();

    void waitReady() throws Exception;

    String accessString();

    default void setMain(String app) {}

    ClusterReceiver getClusterReceiver();
}
