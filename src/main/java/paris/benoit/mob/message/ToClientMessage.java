package paris.benoit.mob.message;

/**
 * Message to be sent to the client.
 */
public class ToClientMessage {
    
    public String table;
    public String jsonPayload;
    
    public ToClientMessage(String table, String jsonPayload) {
        super();
        this.table = table;
        this.jsonPayload = jsonPayload;
    }
    
    public String toString() {
        return "{ \"table\" : \"" + table + "\", \"payload\" : " + jsonPayload + "}" ;
    }
    
}
