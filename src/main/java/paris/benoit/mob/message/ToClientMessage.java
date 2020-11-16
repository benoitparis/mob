package paris.benoit.mob.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Message to be sent to the client.
 */
public class ToClientMessage {

    @JsonProperty
    public String client_id;

    public String table = "";

    @JsonProperty
    public JsonNode payload = null;
    
    public ToClientMessage(String client_id, String table, String payload) {
        this.client_id = client_id;
        this.table = table;
//        this.payload = payload;
    }

    public static ToClientMessage fromString(String value, String tableName) {
        System.out.println(value);
        ObjectMapper mapper = new ObjectMapper();
        try {
            ToClientMessage result = mapper.readValue(value, ToClientMessage.class);
            result.table = tableName;

            return result;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString() {
        return "ToClientMessage{" +
                "to='" + client_id + '\'' +
                ", table='" + table + '\'' +
                ", jsonPayload='" + payload + '\'' +
                '}';
    }

    public String toJson() {
        return "{ \"table\" : \"" + table + "\", \"payload\" : " + payload + "}" ;
    }

    // Jackson en a besoin
    public ToClientMessage() {}

    public void setClient_id(String client_id) {
        this.client_id = client_id;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }

    public String getClient_id() {
        return client_id;
    }

    public JsonNode getPayload() {
        return payload;
    }
}
