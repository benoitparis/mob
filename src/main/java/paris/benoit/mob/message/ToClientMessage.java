package paris.benoit.mob.message;

/**
 * Message to be sent to the client.
 */
public class ToClientMessage {

    public final String to;
    public final String table;
    public final String jsonPayload;
    
    public ToClientMessage(String to, String table, String jsonPayload) {
        this.to = to;
        this.table = table;
        this.jsonPayload = jsonPayload;
    }

    @Override
    public String toString() {
        return "ToClientMessage{" +
                "to='" + to + '\'' +
                ", table='" + table + '\'' +
                ", jsonPayload='" + jsonPayload + '\'' +
                '}';
    }

    public String toJson() {
        return "{ \"table\" : \"" + table + "\", \"payload\" : " + jsonPayload + "}" ;
    }

}
