package paris.benoit.mob.server;

import paris.benoit.mob.message.ToServerMessage;

public interface ClusterSender {

    void sendMessage(ToServerMessage message) throws Exception;

    ToServerMessage receive() throws Exception;

}
