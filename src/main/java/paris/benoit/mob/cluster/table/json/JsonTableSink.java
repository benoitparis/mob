package paris.benoit.mob.cluster.table.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.loopback.ActorSink;

public class JsonTableSink implements RetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSink.class);

    private DataType jsonDataType;
    private static final String[] fieldNames = new String[] {
            "loopback_index",
            "actor_identity",
            "payload"
    };
    private DataType[] fieldTypes;

    private RichSinkFunction<Tuple2<Boolean, Row>> actorFunction;
    private JsonRowSerializationSchema jrs;
    private MobTableConfiguration configuration;

    public JsonTableSink(MobTableConfiguration configuration) {
        jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] {
            DataTypes.INT(),
            DataTypes.STRING(),
            jsonDataType
        };
        logger.info("Created Sink with json schema: " + jsonDataType.toString());

        jrs = new JsonRowSerializationSchema.Builder((TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(jsonDataType)).build();
        actorFunction = new ActorSink(configuration, jrs);
        this.configuration = configuration;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException("Moblib: This class is configured through its constructor");
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        consumeDataStream(ds);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
            .partitionCustom(new IdPartitioner(), it -> (Integer) it.f1.getField(0)) // loopback_index by convention
            .addSink(actorFunction)
            .setParallelism(ds.getExecutionConfig().getMaxParallelism())
//                .getTransformation().setCoLocationGroupKey(configuration.name) // needed ?
            .name(configuration.name);
    }

    // TODO enlever quand ils seront prêt
    @Override
    public TypeInformation<Row> getRecordType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(this.getTableSchema().toRowDataType());
    }

}
