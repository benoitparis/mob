package paris.benoit.mob.cluster.table.js;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

public class JsTableSource implements StreamTableSource<Row> {
    private static final Logger logger = LoggerFactory.getLogger(JsTableSource.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;

    private RichParallelSourceFunction function;
    private MobTableConfiguration configuration;

    public JsTableSource(MobTableConfiguration parentConfiguration, MobTableConfiguration configuration) {

        jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        fieldNames = new String[] {
                "payload"
        };
        fieldTypes = new TypeInformation[] {
                jsonTypeInfo
        };

        function = new JsSourceFunction(parentConfiguration, configuration);
        this.configuration = configuration;
        logger.info("Instanciated JsTableSink with json schema: " + jsonTypeInfo.toString());

    }

    public String getName() {
        return configuration.name;
    }

    @Override
    public String explainSource() {
        return "Json Source";
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(fieldNames, fieldTypes);
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(fieldNames, fieldTypes);
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv
                .addSource(function, getReturnType())
                .setParallelism(1)
                .name(configuration.name);
    }


}
