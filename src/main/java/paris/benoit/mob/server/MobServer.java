package paris.benoit.mob.server;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import paris.benoit.mob.json2sql.JsonTableSink;
import paris.benoit.mob.json2sql.JsonTableSource;

public class MobServer {
    
    // pas encore lib
    public static void main(String[] args) throws Exception {
        launch();
    }
    
    public static void launch() throws Exception {
        UnderTowLauncher.launchUntertow();
        launchStreamAndBlock();
    }
    
    public final static int STREAM_PARALLELISM = 8;
    private static void launchStreamAndBlock() throws Exception {
        
        // Setup
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);
        
        // Processing
        String inSchema = new String(Files.readAllBytes(Paths.get("in.jsonschema")));
        tEnv.registerTableSource("inputTable", new JsonTableSource(inSchema));
        String outSchema = new String(Files.readAllBytes(Paths.get("out.jsonschema")));
        tEnv.registerTableSink("outputTable", new JsonTableSink(outSchema));
        // ici est stream minimal? et on ajoute du ad hoc après?
        String stringSQL = new String(Files.readAllBytes(Paths.get("operation.sql")));
        tEnv.sqlUpdate(stringSQL);
        
        // Launch
        System.out.println("Stream is being initialized. Execution plan: \n"+ sEnv.getExecutionPlan());
        // Blocking until cancellation
        sEnv.execute();
        System.out.println("Stream END");
        
        // Il faudra packager ça:
//            .partitionCustom(new IdPartitioner(), 0)
    }

}
