package paris.benoit.mob.server;

import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.RegistryWeaver;
import paris.benoit.mob.front.FrontActor;
import scala.Option;

public class MobServer {
    
    // pas encore lib
    public static void main(String[] args) throws Exception {
//        launch();
        test();
    }
    
    public static void test() throws Exception {
        UnderTowLauncher.launchUntertow();
        
     // Get the stream and table environments.
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        sEnv.setBufferTimeout(MAX_BUFFER_TIME_MILLIS);
        
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);

        RegistryWeaver registry = new RegistryWeaver(
                sEnv,
                tEnv,
                Paths.get("in.jsonschema"), 
                Paths.get("out.jsonschema"), 
                Paths.get("operation.sql"), 
                Paths.get("query.sql")
        );

        registry.setUpInputOutputTables();

     // Provide a static data set of the rates history table.
        List<Tuple2<Integer, Long>> ratesHistoryData = new ArrayList<>();
        ratesHistoryData.add(Tuple2.of(0, 1L));
        ratesHistoryData.add(Tuple2.of(1, 2L));
        ratesHistoryData.add(Tuple2.of(2, 2L));
        ratesHistoryData.add(Tuple2.of(3, 4L));
        ratesHistoryData.add(Tuple2.of(4, 2L));
        ratesHistoryData.add(Tuple2.of(5, 4L));
        ratesHistoryData.add(Tuple2.of(6, 2L));
        ratesHistoryData.add(Tuple2.of(7, 4L));
        ratesHistoryData.add(Tuple2.of(8, 4L));
        // Create and register an example table using above data set.
        // In the real setup, you should replace this with your own table.
        DataStream<Tuple2<Integer, Long>> ratesHistoryStream = sEnv.fromCollection(ratesHistoryData);
        Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, "currency, r_rate, rowtime.proctime");
        tEnv.registerTable("RatesHistory", ratesHistory);
        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("rowtime", "currency"); // <==== (1)
        tEnv.registerFunction("Rates", rates);
        
        
//        CsvTableSource sourcecsv = new TimestampedCsvTableSource(
//            "testsource.csv", 
//            new String[] { "amount", "currency", "rowtime" }, 
//            new TypeInformation[] { Types.DECIMAL(), Types.DECIMAL(), Types.SQL_TIMESTAMP() }
//        );        
//        tEnv.registerTableSource("Orders", sourcecsv);
        
//        Table query = tEnv.sqlQuery(
//            "SELECT                                     \r\n" + 
//            "  o.amount * r.r_rate AS amount            \r\n" + 
//            "FROM                                       \r\n" + 
//            "  Orders AS o                              \r\n" + 
//            "JOIN LATERAL TABLE (Rates(o.rowtime)) AS r \r\n" + 
//            "  ON r.currency = o.currency               \r\n"
//        );
        

        Table hashInputTable = tEnv.sqlQuery(
            "SELECT loopback_index, actor_identity, payload.X, payload.Y \n" +
            "FROM inputTable"
        );

        DataStream<Row> appendStream = tEnv
            .toAppendStream(hashInputTable, Types.ROW(Types.INT(), Types.STRING(), Types.DECIMAL(), Types.DECIMAL()))
//            .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>()) // nécessaire?
//            .keyBy(0)
            ;

        tEnv.registerTable("keyedInputTable", tEnv.fromDataStream(appendStream, 
                "loopback_index, actor_identity, X, Y, proc_time.proctime"));
        
        Table query = tEnv.sqlQuery(
            "SELECT                                                             \r\n" +
            "  loopback_index,                                                  \r\n" +
            "  actor_identity,                                                  \r\n" +
            "  ROW(X, Y, time_string) payload                                   \r\n" +
            "FROM (                                                             \r\n" +
            "  SELECT                                                           \r\n" +
            "    o.loopback_index                  AS loopback_index,           \r\n" +
            "    o.actor_identity                  AS actor_identity,           \r\n" +
            "    o.X * r.r_rate                    AS X,                        \r\n" +
            "    o.Y                               AS Y,                        \r\n" +
            "    CAST(o.proc_time AS VARCHAR)      AS time_string               \r\n" +
            "  FROM                                                             \r\n" +
            "    keyedInputTable AS o                                           \r\n" +
            "  JOIN LATERAL TABLE (Rates(o.proc_time)) AS r                     \r\n" +
            "    ON r.currency = o.loopback_index                               \r\n" +
            ")                                                                  \r\n"
        );
        
        // TODO générer des dumps auto vers csv/kafka? que tu puisse bolt au runtime pour debug où tu veux sur le pipeline
        tEnv.registerTableSink(
            "outsinkCsv", 
            new String [] { "loopback_index", "actor_identity", "payload" }, 
            new TypeInformation[] { Types.INT(), Types.STRING(), Types.ROW(Types.DECIMAL(), Types.DECIMAL(), Types.STRING()) }, 
            new CsvTableSink("./outregtable.csv", ";", 1, WriteMode.OVERWRITE)
        );

        query.insertInto("outputTable");
        query.insertInto("outsinkCsv");


//        sEnv.execute();

        registry.weaveComponents();
        
        
    }
    
    public static void launch() throws Exception {
//        UnderTowLauncher.launchUntertow();
        launchStreamAndBlock();
    }

    public final static int STREAM_PARALLELISM = 8;
    public final static int MAX_BUFFER_TIME_MILLIS = 5;
    private static void launchStreamAndBlock() throws Exception {
        
        // Setup
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // on fera Ingestion quand on sera capable de faire des event time
        // "Ingestion time is the time that events enter Flink; internally, it is treated similarly to event time."
        // procTime for now
//        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);
        
        // Processing
        // TODO change name?
        RegistryWeaver registry = new RegistryWeaver(
                sEnv,
                tEnv,
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
