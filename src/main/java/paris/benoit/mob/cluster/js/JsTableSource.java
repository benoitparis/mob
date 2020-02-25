package paris.benoit.mob.cluster.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.utils.LegacyDataTypeTransitionUtils;

import javax.annotation.Nullable;

public class JsTableSource implements StreamTableSource<Row>
        , DefinedProctimeAttribute
{
    private static final Logger logger = LoggerFactory.getLogger(JsTableSource.class);

    private static final String[] fieldNames = new String[] {
            "payload",
            "proctime_append_stream"
    };
    private DataType[] fieldTypes;

    private RichParallelSourceFunction<Row> function;
    private MobTableConfiguration configuration;

    public JsTableSource(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {

        DataType jsonDataType = TypeConversions.fromLegacyInfoToDataType(JsonRowSchemaConverter.convert(configuration.content));
        fieldTypes = new DataType[] {
                LegacyDataTypeTransitionUtils.convertDataTypeRemoveLegacy(jsonDataType),
                DataTypes.TIMESTAMP(3),
        };

        function = new JsSourceFunction(parentConfiguration, configuration);
        this.configuration = configuration;
        logger.info("Instanciated JsTableSink with json schema: " + jsonDataType.toString());

    }

    @Override
    public String explainSource() {
        return "Js Engine Source";
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
                .addSource(function, getReturnType())
                .forceNonParallel()
                .name(configuration.fullyQualifiedName());
    }

    public String getName() {
        return configuration.name;
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "proctime_append_stream";
    }

}
