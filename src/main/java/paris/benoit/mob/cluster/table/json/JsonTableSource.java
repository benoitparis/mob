package paris.benoit.mob.cluster.table.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.loopback.ActorSource;

public class JsonTableSource implements StreamTableSource<Row>
//        , DefinedProctimeAttribute
{
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSource.class);

    private DataType jsonDataType;
    private static final String[] fieldNames = new String[] {
            "loopback_index",
            "actor_identity",
            "payload",
            "constant_dummy_source", //TODO remove?
//            "proctime_append_stream"
    };
    private DataType[] fieldTypes;
    
    private RichParallelSourceFunction actorFunction;
    private JsonRowDeserializationSchema jrds;
    private MobTableConfiguration configuration;
    
    public JsonTableSource(MobTableConfiguration configuration) {
        jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] {
            DataTypes.INT(),
            DataTypes.STRING(),
            jsonDataType,
            DataTypes.STRING(),
//            DataTypes.TIMESTAMP(3),
        };
        logger.info("Created Source with json schema: " + jsonDataType.toString());

        jrds = new JsonRowDeserializationSchema.Builder((TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(jsonDataType)).build();
        actorFunction = new ActorSource(configuration, jrds);
        this.configuration = configuration;
    }

    @Override
    public String explainSource() {
        return "Json Source";
    }

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    // TODO faudra virer ça quand ils seront prêt pour les types
    @Override
    public TypeInformation<Row> getReturnType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv
            .addSource(actorFunction, configuration.name, getReturnType())
            //.forceNonParallel()
            .name(configuration.name);
    }
    
    public JsonRowDeserializationSchema getJsonRowDeserializationSchema() {
        return jrds;
    }

    public static int getFieldCount() {
        return fieldNames.length;
    }

//    @Nullable
//    @Override
//    public String getProctimeAttribute() {
//        return "proctime_append_stream";
//    }
}
