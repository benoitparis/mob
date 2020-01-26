package paris.benoit.mob.message;

/**
 * Message provenant au client.
 * @author ben
 *
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
