package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

public class JsTableSink implements RetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSink.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
//    private DataType[] fieldTypes;
    private TypeInformation<?>[] fieldTypesOld;

    private RichSinkFunction actorFunction;
    private MobTableConfiguration configuration;

    public JsTableSink(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration, String invokeFunction, String code) {

        jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        fieldNames = new String[] {
                "insert_time",
                "payload"
        };
        fieldTypesOld = new TypeInformation[] {
                Types.SQL_TIMESTAMP(),
                jsonTypeInfo
        };
        // TODO tester TypeConversions pour aider
//        fieldTypes = new DataType[] {
//                DataTypes.TIMESTAMP(),
//                DataTypes.ANY(jsonTypeInfo)
//        };
        logger.info("Created Js Sink with json schema: " + jsonTypeInfo.toString());

//        nécessaire?
//        jrs = new JsonRowSerializationSchema.Builder(jsonTypeInfo).build();
        actorFunction = new JsSink(parentConfiguration, configuration, invokeFunction, code);
        this.configuration = configuration;
    }

    public String getName() {
        return configuration.name;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException("Moblib: This class is configured through its constructor");
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

//    @Override
//    public TableSchema getTableSchema() {
//        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
//    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypesOld;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return Types.ROW(fieldNames, fieldTypesOld);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        consumeDataStream(ds);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
            .addSink(actorFunction)
            .setParallelism(1)
            .name(configuration.name);
    }


}
