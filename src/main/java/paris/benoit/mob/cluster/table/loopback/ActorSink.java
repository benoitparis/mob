package paris.benoit.mob.cluster.table.loopback;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.paralleluniverse.actors.ActorRef;
import co.paralleluniverse.actors.ActorRegistry;

@SuppressWarnings("serial")
public class ActorSink extends RichSinkFunction<Row> {
    private static final Logger logger = LoggerFactory.getLogger(ActorSink.class);
    
    private JsonRowSerializationSchema jrs = null;
    private Integer loopbackIndex = -1;
    
    public ActorSink(TypeInformation<Row> jsonTypeInfo) {
        this.jrs = new JsonRowSerializationSchema(jsonTypeInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: sources and sinks of same index will be co-located
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("Opening sink #" + loopbackIndex);
    }
    
    @Override
    public void invoke(Row row) throws Exception {
        
        Integer loopbackIndex = (Integer) row.getField(0);
        if (loopbackIndex != this.loopbackIndex) {
            // Logging
            logger.error("Assumption broken on lookbackIndex: " + loopbackIndex + " vs " + this.loopbackIndex);
        }
        // par convention? faudrait faire par nom?
        String identity = (String) row.getField(1);
        // arreter de faire par convention, le vrai schema est pas loin
        Row payload = (Row) row.getField(2);
        
        // ICI: faire un petit coup de metadata pour donner le name de this? 
        
        String payloadString = new String(jrs.serialize(payload));
        final ActorRef<String> actor = (ActorRef<String>) ActorRegistry.tryGetActor(identity);
        if (null != actor) {
            // call to send: not blocking or dropping the message, as his mailbox is unbounded
            actor.send(payloadString);
        } else {
            logger.error("Actor named " + identity + " was not found on sink #" + loopbackIndex);
        }

        logger.debug("new msg in sink: " + row);
    }

}
