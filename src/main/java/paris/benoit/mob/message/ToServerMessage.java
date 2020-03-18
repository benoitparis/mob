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
    public enum INTENT {WRITE, QUERY, SUBSCRIBE}

    public final INTENT intent;
    public String from;
    public final String table;
    public final JSONObject payload;
    
    public ToServerMessage(String from, String fromClient) {
        this.from = from;
        JSONObject json = new JSONObject(fromClient);
        this.intent = INTENT.valueOf(json.getString("intent"));
        this.table = json.getString("table");
        this.payload = json.getJSONObject("payload");
    }

    @Override
    public String toString() {
        return "ToServerMessage{" +
                "intent=" + intent +
                ", from='" + from + '\'' +
                ", table='" + table + '\'' +
                ", payload=" + payload +
                '}';
    }
}
