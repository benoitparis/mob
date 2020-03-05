package paris.benoit.mob.cluster.js;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.TypedRetractStreamTableSink;

class JsTableSink extends TypedRetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSink.class);

    public JsTableSink(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration, String invokeFunction, String code) {
        fieldNames = new String[] { "payload" };
        DataType jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] {jsonDataType};
        sinkFunction = new JsSinkFunction(parentConfiguration, configuration, invokeFunction, code);
        name = configuration.fullyQualifiedName();
        logger.info("Instanciated JsTableSink with json schema: " + jsonDataType.toString());
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
            .addSink(sinkFunction)
            .setParallelism(1)
            .name(name);
    }


}
