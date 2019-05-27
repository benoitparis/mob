package paris.benoit.mob.cluster.table.json;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.loopback.ActorSource;

public class JsonTableSource implements StreamTableSource<Row> {
    
    private static final Logger logger = LoggerFactory.getLogger(JsonTableSource.class);

    private TypeInformation<Row> jsonTypeInfo;
    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    
    private ActorSource actorFunction;
    private JsonRowDeserializationSchema jrds;
    
    public JsonTableSource(MobTableConfiguration configuration) {
        jsonTypeInfo = JsonRowSchemaConverter.convert(configuration.ddl);
        fieldNames = new String[] { 
            "loopback_index",
            "actor_identity",
            "payload"
        };
        fieldTypes = new TypeInformation[] {
            Types.INT(),
            Types.STRING(),
            jsonTypeInfo
        };
        logger.info("Created Source with json schema: " + jsonTypeInfo.toString());

        jrds = new JsonRowDeserializationSchema(jsonTypeInfo);
        actorFunction = new ActorSource(configuration, jrds);
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
        return sEnv .addSource(actorFunction, getReturnType());
    }
    
    public JsonRowDeserializationSchema getJsonRowDeserializationSchema() {
        return jrds;
    }

}
