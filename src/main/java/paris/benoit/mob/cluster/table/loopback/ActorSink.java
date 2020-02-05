package paris.benoit.mob.cluster.table.loopback;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.MessageRouter;

@SuppressWarnings("serial")
public class ActorSink extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(ActorSink.class);
    
    private Integer loopbackIndex = -1;
    private JsonRowSerializationSchema jrs;
    private MobTableConfiguration configuration;
    private MessageRouter router;
    
    public ActorSink(MobTableConfiguration configuration, JsonRowSerializationSchema jrs, MessageRouter router) {
        super();
        this.jrs = jrs;
        this.configuration = configuration;
        this.router = router;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: sources and sinks of same index will be co-located
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("Opening sink #" + loopbackIndex);
    }
    
    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {

        //noinspection StatementWithEmptyBody
        if (value.f0) { // Add
            
            Row row = value.f1;
            
            // By convention
            Integer loopbackIndex = (Integer) row.getField(0);
            String identity = (String) row.getField(1);
            Row payload = (Row) row.getField(2);

            if (!loopbackIndex.equals(this.loopbackIndex)) {
                logger.error("Assumption broken on lookbackIndex: " + loopbackIndex + " vs " + this.loopbackIndex);
            }
            
            String payloadString = new String(jrs.serialize(payload));
            
            ToClientMessage message = new ToClientMessage(configuration.name, payloadString);

            router.routeMessage(loopbackIndex, identity, message);

            //logger.debug("new msg in sink: " + row);
            
        } else { 
            // Retract. Do nothing
        }

    }



}
