package paris.benoit.mob.server;

public interface ClusterFront {

    void start();

    void waitReady() throws InterruptedException;

    String accessString();

    default void setMain(String app) {}
}
