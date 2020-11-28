package paris.benoit.mob.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.json.JSONObject;

/**
 * Message from a client, destined to a Table.
 */
public class ToServerMessage {

    @JsonProperty
    public final String client_id;

    @JsonProperty
    public final String table;

    // TODO renommer payload en value
    // TODO ajouter key, vérifier que pas null
    // TODO mettre de l'avro, vérifier que respecte le schema
    @JsonProperty
    public final JSONObject payload;
    
    public ToServerMessage(String client_id, String fromClient) {
        this.client_id = client_id;
        JSONObject json = new JSONObject(fromClient);
        this.table = json.getString("table");
        this.payload = json.getJSONObject("payload");
    }

    // TODO proper schema management
    public String toJsonString() {
        return "{" +
                "\"client_id\":\"" + client_id + '\"' +
                ", \"payload\":" + payload +
                '}';
    }

    @Override
    public String toString() {
        return "ToServerMessage{" +
                "client_id='" + client_id + '\'' +
                ", table='" + table + '\'' +
                ", payload=" + payload +
                '}';
    }
}
