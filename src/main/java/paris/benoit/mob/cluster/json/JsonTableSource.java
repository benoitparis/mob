package paris.benoit.mob.cluster.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import paris.benoit.mob.cluster.loopback.ActorSource;
import paris.benoit.mob.cluster.utils.LegacyDataTypeTransitionUtils;

public class JsonTableSource implements StreamTableSource<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSource.class);

    private static final String[] fieldNames = new String[] {
            "loopback_index",
            "actor_identity",
            "payload",
            "constant_dummy_source", //TODO remove?
            "unix_time_insert"
    };
    private final DataType[] fieldTypes;
    
    private final RichParallelSourceFunction<Row> actorFunction;
    private final MobTableConfiguration configuration;
    
    public JsonTableSource(MobTableConfiguration configuration) {
        DataType tempType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));

        DataType jsonDataType = LegacyDataTypeTransitionUtils.convertDataTypeRemoveLegacy(tempType);

        fieldTypes = new DataType[] {
            DataTypes.INT(),
            DataTypes.STRING(),
            jsonDataType,
            DataTypes.STRING(),
            DataTypes.BIGINT()
        };
        logger.info("Created Source with json schema: " + jsonDataType.toString());

        actorFunction = new ActorSource(configuration, jsonDataType);
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
            .name(configuration.fullyQualifiedName());
    }

    public static int getFieldCount() {
        return fieldNames.length;
    }

}
