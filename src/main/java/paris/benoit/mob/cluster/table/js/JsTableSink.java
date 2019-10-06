package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.IdPartitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.loopback.ActorSink;

public class JsTableSink implements RetractStreamTableSink<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSink.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    private RichSinkFunction actorFunction;
    private MobTableConfiguration configuration;

    public JsTableSink(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {

        jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        fieldNames = new String[] {
                "insert_time",
                "payload"
        };
        fieldTypes = new TypeInformation[] {
                Types.SQL_TIMESTAMP(),
                jsonTypeInfo
        };
        logger.info("Created Js Sink with json schema: " + jsonTypeInfo.toString());

//        nécessaire?
//        jrs = new JsonRowSerializationSchema.Builder(jsonTypeInfo).build();
        actorFunction = new JsSink(parentConfiguration, configuration);
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

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return Types.ROW(fieldNames, fieldTypes);
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
