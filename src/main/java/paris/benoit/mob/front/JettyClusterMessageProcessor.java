package paris.benoit.mob.front;

import paris.benoit.mob.message.ToClientMessage;

public interface JettyClusterMessageProcessor {
    void processServerMessage(ToClientMessage message);
}
