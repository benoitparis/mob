package paris.benoit.mob.server;

import java.nio.file.Paths;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.front.FrontActor;

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
//        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        
        // Processing
        // TODO change name?
        RegistryWeaver registry = new RegistryWeaver(
                sEnv,
                Paths.get("in.jsonschema"), 
                Paths.get("out.jsonschema"), 
                Paths.get("operation.sql")
        );
        registry.weaveComponents();
        

        
        // Il faudra packager ça:
//            .partitionCustom(new IdPartitioner(), 0)
    }

}
