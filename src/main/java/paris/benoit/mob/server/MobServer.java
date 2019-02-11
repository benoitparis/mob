package paris.benoit.mob.server;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.front.FrontActor;

public class MobServer {
    
    // pas encore lib
    public static void main(String[] args) throws Exception {
        launch();
//        test();
    }
    
    public static void test() throws Exception {
     // Get the stream and table environments.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        // Provide a static data set of the rates history table.
        List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
        ratesHistoryData.add(Tuple2.of("Euro", 114L));
        ratesHistoryData.add(Tuple2.of("Yen", 1L));
        ratesHistoryData.add(Tuple2.of("Euro", 116L));
        ratesHistoryData.add(Tuple2.of("Euro", 119L));

        // Create and register an example table using above data set.
        // In the real setup, you should replace this with your own table.
        DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "r_currency, r_rate, r_proctime.proctime");

        tEnv.registerTable("RatesHistory", ratesHistory);

        // Create and register a temporal table function.
        // Define "r_proctime" as the time attribute and "r_currency" as the primary key.
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
        tEnv.registerFunction("Rates", rates);
        
        CsvTableSource sourcecsv = new TsCsvTbSource(
            "testsource.csv", 
            new String[] { "amount", "currency", "rowtime" }, 
            new TypeInformation[] { Types.INT(), Types.STRING(), Types.SQL_TIMESTAMP() }
        );
        
        tEnv.registerTableSource("Orders", sourcecsv);
        
        Table query = tEnv.sqlQuery(
            "SELECT                                    \r\n" + 
            "  o.amount * r.r_rate AS amount           \r\n" + 
            "FROM                                      \r\n" + 
            "  Orders AS o,                            \r\n" + 
            "  LATERAL TABLE (Rates(o.rowtime)) AS r   \r\n" + 
            "WHERE r.r_currency = o.currency           \r\n"
        );

        tEnv.registerTableSink("outsink", new CsvTableSink("./outtable.csv", ";", 1, WriteMode.OVERWRITE) {
            @Override
            public String[] getFieldNames() {
                return new String [] {"amount"};
            };
            @Override
            public TypeInformation<?>[] getFieldTypes() {
                return new TypeInformation[] { Types.LONG() };
            }
        });
        query.insertInto("outsink");


        env.execute();
    }
    
    public static void launch() throws Exception {
        UnderTowLauncher.launchUntertow();
        launchStreamAndBlock();
    }
    
    public final static int STREAM_PARALLELISM = 8;
    private static void launchStreamAndBlock() throws Exception {
        
        // Setup
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // on fera Ingestion quand on sera capable de faire des event time
        // "Ingestion time is the time that events enter Flink; internally, it is treated similarly to event time."
        // procTime for now
//        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        
        // Processing
        // TODO change name?
        RegistryWeaver registry = new RegistryWeaver(
                sEnv,
                Paths.get("in.jsonschema"), 
                Paths.get("out.jsonschema"), 
                Paths.get("operation.sql"), 
                Paths.get("query.sql")
        );
        registry.weaveComponents();
        

        
        // Il faudra packager ça:
//            .partitionCustom(new IdPartitioner(), 0)
    }

}
