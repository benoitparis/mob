package paris.benoit.mob.message;

import org.json.JSONObject;

public class ClientMessage {
    
    // INFO on met quoi? des ACKs de choses? 
    public enum INTENT {QUERY, CALL, SUBSCRIBE, INFO};
    
    public INTENT intent;
    public JSONObject payload;
    
    public ClientMessage(String fromClient) {
        JSONObject json = new JSONObject(fromClient);
        intent = INTENT.valueOf(json.getString("INTENT"));
        payload = json.getJSONObject("PAYLOAD");
    }

}
