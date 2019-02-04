package paris.benoit.mob.message;

import org.apache.flink.types.Row;

public class InputRow {
    
    // on connait pas le loopback à la formation du message, serait à peine bien pour le sink
    // en tout cas, il nous faut une notion de message in sous forme de row bien foutue
    //   [on supprime le loobpack pour que compile passe: tuple2(int,row)->row; et on perd info identité
    private String identity;
    private Integer loopbackIndex;
    private Row payload;
    
    public InputRow(String identity, Row payload) {
        super();
        this.identity = identity;
        this.payload = payload;
    }
    
    public void setLoopbackIndex(Integer loopbackIndex) {
        this.loopbackIndex = loopbackIndex;
    }

    public Row toFlinkRow() {
        Row root = new Row(1);
        root.setField(0, payload);
        return root;
        
    }

    
}
