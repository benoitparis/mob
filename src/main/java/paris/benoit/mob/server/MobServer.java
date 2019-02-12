package paris.benoit.mob.server;

import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import paris.benoit.mob.cluster.ClusterRegistry;

public class MobServer {
    
    // pas encore lib
    public static void main(String[] args) throws Exception {
        UnderTowLauncher.launchUntertow();
        setupCluster();
    }
    

    public final static int STREAM_PARALLELISM = 8;
    public final static int MAX_BUFFER_TIME_MILLIS = 5;
    
    public static void setupCluster() throws Exception {
        
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        sEnv.setBufferTimeout(MAX_BUFFER_TIME_MILLIS);
        
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);

        // source name, source list? source schema
        // temporal state list, 
        // query source list?
        // write source list? // les mêmes en source list? // tt le temps associé à schema?
        // explicit queries list
        // sink list?
        //   avec du routage coté acteur cluster msg out et le ClusterSender/Port route avant envoi?
        
        String inSchema = new String(Files.readAllBytes(Paths.get("in.jsonschema")));
        String outSchema = new String(Files.readAllBytes(Paths.get("out.jsonschema")));

        String stateSql = new String(Files.readAllBytes(Paths.get("operation.sql")));
        String querySql = new String(Files.readAllBytes(Paths.get("query.sql")));

        ClusterRegistry registry = new ClusterRegistry(
            sEnv,
            tEnv,
            inSchema,
            "write_position",
            outSchema,
            "send_client",
            new String[] { stateSql }, 
            new String[] { querySql }
        );
        registry.weaveComponents();
        
        // éviter la god class: mettre tous les static nécessaires ici?
        //   ou bien dans le package: pareil que UnderTow, un objet static ClusterMob
        
        
    }
    



}
