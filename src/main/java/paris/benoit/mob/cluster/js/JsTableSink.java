package paris.benoit.mob.cluster.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

public class JsTableSink implements RetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSink.class);

    private DataType jsonDataType;
    private static final String[] fieldNames = new String[] { "payload" };
    private DataType[] fieldTypes;

    private RichSinkFunction<Tuple2<Boolean, Row>> function;
    private MobTableConfiguration configuration;

    public JsTableSink(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration, String invokeFunction, String code) {

        jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] { jsonDataType };

        function = new JsSinkFunction(parentConfiguration, configuration, invokeFunction, code);
        this.configuration = configuration;
        logger.info("Instanciated JsTableSink with json schema: " + jsonDataType.toString());
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
            .addSink(function)
            .setParallelism(1)
            .name(configuration.fullyQualifiedName());
    }

    // TODO enlever quand ils seront prêt
    @Override
    public TypeInformation<Row> getRecordType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(this.getTableSchema().toRowDataType());
    }

    public String getName() {
        return configuration.name;
    }

}
