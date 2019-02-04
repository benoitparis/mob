package paris.benoit.mob.message;

public class ClusterMessage {
    
    public String loopbackAdress;
    Message payload;
    
    public ClusterMessage(String loopbackAdress, Message payload) {
        super();
        this.loopbackAdress = loopbackAdress;
        this.payload = payload;
    }
    
    public String getLoopbackAdress() {
        return loopbackAdress;
    }

    public Message getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "ClusterMessage [loopbackAdress=" + loopbackAdress + ", payload=" + payload + "]";
    }
    
}
