package paris.benoit.mob.cluster.table.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.loopback.ActorSource;

import javax.annotation.Nullable;

public class JsonTableSource implements StreamTableSource<Row>, DefinedProctimeAttribute {
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSource.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    
    private RichParallelSourceFunction actorFunction;
    private JsonRowDeserializationSchema jrds;
    private MobTableConfiguration configuration;
    
    public JsonTableSource(MobTableConfiguration configuration) {
        jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.content);
        fieldNames = new String[] { 
            "loopback_index",
            "actor_identity",
            "payload",
            "constant_dummy_source", //TODO remove?
            "proctime" // TODO on clarifie le nom avec <le nom de la table>_time ? ça aidera pr les ambiguités
                // TODO mettre avant le payload
        };
        fieldTypes = new TypeInformation[] {
            Types.INT(),
            Types.STRING(),
            jsonTypeInfo,
            Types.STRING(),
            Types.SQL_TIMESTAMP()
        };
        logger.info("Created Source with json schema: " + jsonTypeInfo.toString());

        jrds = new JsonRowDeserializationSchema.Builder(jsonTypeInfo).build();
        actorFunction = new ActorSource(configuration, jrds);
        this.configuration = configuration;
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
        return sEnv .addSource(actorFunction, getReturnType())
                    .name(configuration.name);
    }
    
    public JsonRowDeserializationSchema getJsonRowDeserializationSchema() {
        return jrds;
    }

    @Nullable
    @Override
    public String getProctimeAttribute() {
        return "proctime";
    }
}
