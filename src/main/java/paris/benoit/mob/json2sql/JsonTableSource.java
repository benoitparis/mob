package paris.benoit.mob.json2sql;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSchemaConverter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import co.paralleluniverse.fibers.SuspendExecution;
import paris.benoit.mob.loopback.ActorSource;

public class JsonTableSource implements StreamTableSource<Row> {
    
    // sale cette classe est pas serializable (le cow?)
    //   puis on fait un random pour au final avoir un gros static
    //   faudrait se faire une liste de ce qui est par acteur, par thread, par info métier, 
    //     de par qui va se faire serializer, de timeline sur objet, de convention, etc
    //       un peu à la TLAps où une solution est une state machine bien simple
    ActorSource actorFunction;
    TypeInformation<Row> jsonTypeInfo;
    JsonRowDeserializationSchema jrds;
    
    static JsonTableSource it;
    static volatile int parallelism = -1;
    static CopyOnWriteArrayList<ActorSource> childFunctions = new CopyOnWriteArrayList<>();
    
    public JsonTableSource(String schema) {
        actorFunction = new ActorSource(this);
        jsonTypeInfo = JsonRowSchemaConverter.convert(schema);
        jrds = new JsonRowDeserializationSchema(jsonTypeInfo);
        // singleton for now, we'll see later about available tables to actors
        it = this;
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
        parallelism = sEnv.getParallelism();
        return sEnv.addSource(actorFunction, getReturnType());
    }
    
    public void emitRow(String identity, String payload) throws SuspendExecution, InterruptedException {

        try {
            Row payloadRow = jrds.deserialize(payload.getBytes());
            Row root = new Row(3);
            // 0 is loopbackIndex, by convention; to be set by the function
            root.setField(1, identity);
            root.setField(2, payloadRow);
            
            int index = ThreadLocalRandom.current().nextInt(0, childFunctions.size());
            ActorSource chosenFunction = childFunctions.get(index);
            
            chosenFunction.emitRow(root);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    public static JsonTableSource getInstance() throws InterruptedException {
        do {
            Thread.sleep(100);
        } while (childFunctions.size() != parallelism);
        return it;
    }
    
    public void registerChildFunction(ActorSource sourceFunction) {
        childFunctions.add(sourceFunction);
    }

}
