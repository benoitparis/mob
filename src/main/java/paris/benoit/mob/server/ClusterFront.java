package paris.benoit.mob.server;

public interface ClusterFront {

    void start();

    void waitReady() throws InterruptedException;

    public String accessString();
}
