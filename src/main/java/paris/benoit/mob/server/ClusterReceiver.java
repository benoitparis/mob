package paris.benoit.mob.server;

import paris.benoit.mob.message.ToClientMessage;

import java.io.Serializable;

public interface ClusterReceiver extends Serializable {

    void receiveMessage(Integer loopbackIndex, String identity, ToClientMessage message);

}
