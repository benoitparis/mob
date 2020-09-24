package paris.benoit.mob.cluster.js;

import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.RowStreamTableSource;
import paris.benoit.mob.cluster.utils.LegacyDataTypeTransitionUtils;

import javax.annotation.Nullable;

class JsTableSource extends RowStreamTableSource implements DefinedProctimeAttribute {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSource.class);

    public JsTableSource(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {
        fieldNames = new String[]{
                "payload",
                "proctime_append_stream"
        };
        DataType jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[]{
                LegacyDataTypeTransitionUtils.convertDataTypeRemoveLegacy(jsonDataType),
                DataTypes.TIMESTAMP(3),
        };
        sourceFunction = new JsSourceFunction(parentConfiguration, configuration);
        name = "JS Engine Source: " + configuration.fullyQualifiedName();

        logger.info("Instanciated JsTableSink with json schema: " + jsonDataType.toString());
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv
                .addSource(sourceFunction, getReturnType())
                .forceNonParallel()
                .name(explainSource());
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "proctime_append_stream";
    }

}
