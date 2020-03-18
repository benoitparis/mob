package paris.benoit.mob.message;

import org.json.JSONObject;

/**
 * Message from a client, destined to a Table.
 */
public class ToServerMessage {

    public String from;
    public final String table;
    public final JSONObject payload;
    
    public ToServerMessage(String from, String fromClient) {
        this.from = from;
        JSONObject json = new JSONObject(fromClient);
        this.table = json.getString("table");
        this.payload = json.getJSONObject("payload");
    }

    @Override
    public String toString() {
        return "ToServerMessage{" +
                "from='" + from + '\'' +
                ", table='" + table + '\'' +
                ", payload=" + payload +
                '}';
    }
}
