package paris.benoit.mob.cluster.json2sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import paris.benoit.mob.cluster.loopback.ActorSource;

public class JsonTableSource implements StreamTableSource<Row> {
    
    // sale cette classe est pas serializable (le cow?)
    //   puis on fait un random pour au final avoir un gros static
    //   faudrait se faire une liste de ce qui est par acteur, par thread, par info métier, 
    //     de par qui va se faire serializer, de timeline sur objet, de convention, etc
    //       un peu à la TLAps où une solution est une state machine bien simple
    ActorSource actorFunction;
    TypeInformation<Row> jsonTypeInfo;
    JsonRowDeserializationSchema jrds;
    
    public JsonTableSource(String schema) {
        actorFunction = new ActorSource(this);
        jsonTypeInfo = JsonRowSchemaConverter.convert(schema);
        jrds = new JsonRowDeserializationSchema(jsonTypeInfo);
    }

    @Override
    public String explainSource() {
        return "Json source";
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return Types.ROW(
            new String[] {
                "loopback_index",
                "actor_identity",
                "payload",
            }, 
            new TypeInformation[] {
                Types.STRING(),
                Types.STRING(),
                jsonTypeInfo,
            }
        );
    }

    // TODO DRY
    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
            .field("loopback_index", Types.STRING())
            .field("actor_identity", Types.STRING())
            .field("payload", jsonTypeInfo)
            .build();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment sEnv) {
        return sEnv.addSource(actorFunction, getReturnType());
    }
    
    public JsonRowDeserializationSchema getJsonRowDeserializationSchema() {
        return jrds;
    }

}
