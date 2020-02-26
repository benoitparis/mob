package paris.benoit.mob.server;

import java.util.List;

public interface ClusterRunner {

    void run(List<String> apps) throws Exception;
}
