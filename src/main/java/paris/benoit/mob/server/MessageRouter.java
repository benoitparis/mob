package paris.benoit.mob.server;

import paris.benoit.mob.message.ToClientMessage;

import java.io.Serializable;

public interface MessageRouter extends Serializable {

    void routeMessage(Integer loopbackIndex, String identity, ToClientMessage message);

}
