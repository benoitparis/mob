package paris.benoit.mob.cluster.loopback;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.ActorRegistry;
import paris.benoit.mob.cluster.RegistryWeaver;

@SuppressWarnings("serial")
public class ActorSink extends RichSinkFunction<Row> {
    
    private JsonRowSerializationSchema jrs = null;
    private Integer loopbackIndex = -1;
    
    public ActorSink(TypeInformation<Row> jsonTypeInfo) {
        this.jrs = new JsonRowSerializationSchema(jsonTypeInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        loopbackIndex = RegistryWeaver.registerSink(this);
    }
    
    @Override
    public void invoke(Row row) throws Exception {
        
        String loopbackIndex = (String) row.getField(0);
        if (Integer.parseInt(loopbackIndex) != this.loopbackIndex) {
            // Logging
            System.out.println("Assumption broken on lookbackIndex");
        }
        // par convention? faudrait faire par nom?
        String identity = (String) row.getField(1);
        // arreter de faire par convention, le vrai schema est pas loin
        Row payload = (Row) row.getField(2);
        String payloadString = new String(jrs.serialize(payload));
        // call to send: not blocking or dropping the message, as his mailbox is unbounded
        ((ActorRef<String>)ActorRegistry.getActor(identity)).send(payloadString);

        System.out.println("new msg in sink: " + row);
    }

}
