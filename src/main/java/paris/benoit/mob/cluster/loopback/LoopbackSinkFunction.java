package paris.benoit.mob.cluster.loopback;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.message.ToClientMessage;
import paris.benoit.mob.server.ClusterReceiver;

@SuppressWarnings("serial")
class LoopbackSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
    private static final Logger logger = LoggerFactory.getLogger(LoopbackSinkFunction.class);
    
    private Integer loopbackIndex = -1;
    private final JsonRowSerializationSchema jrs;
    private final MobTableConfiguration configuration;
    private final ClusterReceiver receiver;
    
    public LoopbackSinkFunction(MobTableConfiguration configuration, DataType jsonDataType, ClusterReceiver receiver) {
        super();
        this.jrs = new JsonRowSerializationSchema.Builder((TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(jsonDataType)).build();
        this.configuration = configuration;
        this.receiver = receiver;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Assumption: sources and sinks of same index will be co-located
        loopbackIndex = getRuntimeContext().getIndexOfThisSubtask();
        logger.info("Opening sink #" + loopbackIndex);
    }
    
    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) {

        if (value.f0) {
            
            Row row = value.f1;
            Integer loopbackIndex = (Integer) row.getField(0);
            String identity = (String) row.getField(1);
            Row payload = (Row) row.getField(2);

            if (!loopbackIndex.equals(this.loopbackIndex)) {
                logger.error("Assumption broken on lookbackIndex: " + loopbackIndex + " vs " + this.loopbackIndex);
            }

            ToClientMessage message = new ToClientMessage(configuration.name, new String(jrs.serialize(payload)));
            receiver.receiveMessage(loopbackIndex, identity, message);
            //logger.debug("new msg in sink: " + row);
            
        }
        // else {} Retract. Do nothing
    }

}
