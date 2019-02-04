package paris.benoit.mob.message;

import org.json.JSONObject;

/**
 * Message provenant du client.
 * @author ben
 *
 */
public class ClientMessage {
    
    // INFO on met quoi? des ACKs de choses? 
    // on va s'orienter vers QUERY, SUBSCRIBE, WRITE, avec potentiellement les mêmes conventions que graphql sur 
    //   les write / les mutation (qui ne seront que des append immutables ici) qui peuvent yield un résultat qui
    //   ressemblerait à une QUERY
    public enum INTENT {QUERY, CALL, SUBSCRIBE, INFO};
    
    public INTENT intent;
    public JSONObject payload;
    
    public ClientMessage(String fromClient) {
        JSONObject json = new JSONObject(fromClient);
        intent = INTENT.valueOf(json.getString("INTENT"));
        payload = json.getJSONObject("PAYLOAD");
    }

}
