package paris.benoit.mob.message;

import org.json.JSONObject;

/**
 * Message provenant du client.
 * @author ben
 *
 */
public class ToServerMessage {
    
    // INFO on met quoi? des ACKs de choses? 
    // on va s'orienter vers QUERY, SUBSCRIBE, WRITE, avec potentiellement les mêmes conventions que graphql sur 
    //   les write / les mutation (qui ne seront que des append immutables ici) qui peuvent yield un résultat qui
    //   ressemblerait à une QUERY
    // TODO transformer en INSERT, DELETE, UPDATE?
    //   ou bien en APPEND, RETRACT(, UPSERT)?
    public enum INTENT {WRITE, QUERY, SUBSCRIBE};

    public INTENT intent;
    public String table;
    public JSONObject payload;
    
    public ToServerMessage(String fromClient) {
        JSONObject json = new JSONObject(fromClient);
        intent = INTENT.valueOf(json.getString("intent"));
        table = json.getString("table");
        payload = json.getJSONObject("payload");
    }

    @Override
    public String toString() {
        return "ToServerMessage{" +
                "intent=" + intent +
                ", table='" + table + '\'' +
                ", payload=" + payload +
                '}';
    }
}
