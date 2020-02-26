package paris.benoit.mob.cluster.json;

import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.TypedRetractStreamTableSink;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.loopback.ActorSink;
import paris.benoit.mob.server.MessageRouter;

public class JsonTableSink extends TypedRetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSink.class);

    private static final String[] FIELD_NAMES = new String[] {
            "loopback_index",
            "actor_identity",
            "payload"
    };

    public JsonTableSink(MobTableConfiguration configuration, MessageRouter router) {
        fieldNames = FIELD_NAMES;
        DataType jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] {
            DataTypes.INT(),
            DataTypes.STRING(),
            jsonDataType
        };
        logger.info("Created Sink with json schema: " + jsonDataType.toString());
        sinkFunction = new ActorSink(configuration, jsonDataType, router);
        name = configuration.fullyQualifiedName();
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
            .partitionCustom(new IdPartitioner(), it -> (Integer) it.f1.getField(0)) // loopback_index by convention
            .addSink(sinkFunction)
            .setParallelism(ds.getExecutionConfig().getMaxParallelism())
//                .getTransformation().setCoLocationGroupKey(configuration.name) // needed ?
            .name(name);
    }


}
