package paris.benoit.mob.message;

public class ClusterMessage {
    
    // on connait pas le loopback à la formation du message, serait à peine bien pour le sink
    // en tout cas, il nous faut une notion de message in sous forme de row bien foutue
    //   [on supprime le loobpack pour que compile passe: tuple2(int,row)->row; et on perd info identité
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
