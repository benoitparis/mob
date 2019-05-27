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
    // et le routage vers les divers types?
    // faudrait une nomenclature
    public enum INTENT {WRITE, QUERY, SUBSCRIBE};

    public INTENT intent;
    public String destination;
    public JSONObject payload;
    
    public ClientMessage(String fromClient) {
        JSONObject json = new JSONObject(fromClient);
        intent = INTENT.valueOf(json.getString("INTENT"));
        destination = json.getString("destination");
        payload = json.getJSONObject("PAYLOAD");
    }

}
