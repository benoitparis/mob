package paris.benoit.mob.cluster.loopback;

import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.TypedStreamTableSource;
import paris.benoit.mob.cluster.utils.LegacyDataTypeTransitionUtils;

public class JsonTableSource extends TypedStreamTableSource<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSource.class);

    public static final String COLOCATION_KEY = "CLIENT_LOOPBACK";

    private static final String[] FIELD_NAMES = new String[] {
            "loopback_index",
            "actor_identity",
            "payload",
            "constant_dummy_source",
            "unix_time_insert"
    };
    public static final int FIELD_COUNT = FIELD_NAMES.length;

    private MobTableConfiguration configuration;

    public JsonTableSource(MobTableConfiguration configuration) {
        fieldNames = FIELD_NAMES;
        DataType tempType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        DataType jsonDataType = LegacyDataTypeTransitionUtils.convertDataTypeRemoveLegacy(tempType);
        fieldTypes = new DataType[] {
            DataTypes.INT(),
            DataTypes.STRING(),
            jsonDataType,
            DataTypes.STRING(),
            DataTypes.BIGINT()
        };
        sourceFunction = new ActorSource(configuration, jsonDataType);
        name = configuration.fullyQualifiedName();
        this.configuration = configuration;
        logger.info("Created Source with json schema: " + jsonDataType.toString());

    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        SingleOutputStreamOperator<Row> dataStream = sEnv
                .addSource(sourceFunction, configuration.name, getReturnType())
                .setParallelism(sEnv.getMaxParallelism())
                .name(explainSource());
        dataStream.getTransformation().setCoLocationGroupKey(COLOCATION_KEY);
        return dataStream;
    }

}
