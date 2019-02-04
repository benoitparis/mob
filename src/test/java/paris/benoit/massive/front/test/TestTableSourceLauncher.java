package paris.benoit.massive.front.test;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.calcite.schema.Table;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;

import paris.benoit.mob.json2sql.JsonTableSink;
import paris.benoit.mob.json2sql.JsonTableSource;

public class TestTableSourceLauncher {
    
    public static final int STREAM_PARALLELISM = 8;
    
    public static void main(String[] args) throws Exception {
        
        // DONE now on a le json qui marche, mais avec description partiellement en dur du schema.
        // DONE next step: tenter go one level deeper, et avoir un champ payload de type row dans le table schema, mais c'est bien hierarchique et on a tous les niveaux du json 
        // DONE next step: (pas forcément en deuxième position) avoir la conf du payload dans un schema et faire un constructeur etc au lieu de relire 5 fois
        // DONE next step: full sql loadé depuis l'extérieur (\" tedious), plus ça se rapproche de lib
        // DONE next step: nested json
        // DONE au final c'est fait dans la syntaxe SQL pur. Se méfier que SQL SELECT expressions != table.select() expressions
        //   DEPR V+ next step: get: voir si on peut pas avoir les fields en tant que fonction, pour faire du payload.col1 ou au pire du payload.col1(). Voter V+ BRANCH pas du tout prioritaire
        // next step: package as a library en donnant schema et connections aux sourcefunctions
        // next step: définir le schema de sortie de la lib
        // next step: faire un hello world pur processingjs
        // next step: définir la structure des queries (et on les oneshot/subscribe?); pareil pour les writes, qui renvoient peut etre des choses à la graphql. au moins un ACK? 
        // DONE next step: tester si on peut syntax du row hierarchique et le sink
        //   difficile, résultats mitigés -> pollution schema sink expect flat?
        // DONE next step: faire un tablesink
        // DONE next step: base tablesink
        // DONE next step: nested tablesink
        // REM ROW faut lui parler gentiment
        //   https://stackoverflow.com/questions/54224170/using-row-for-nested-data-structure
        //   https://issues.apache.org/jira/projects/FLINK/issues/FLINK-11399
        
        
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(sEnv);
        
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        sEnv.setParallelism(STREAM_PARALLELISM);
        
        String inSchema = new String(Files.readAllBytes(Paths.get("in.jsonschema")));
        tEnv.registerTableSource("inputTable", new JsonTableSource(inSchema));
        String outSchema = new String(Files.readAllBytes(Paths.get("out.jsonschema")));
        tEnv.registerTableSink("outputTable", new JsonTableSink(outSchema));
        
        System.out.println(Arrays.asList(tEnv.listTables()));
        final Table iTable = tEnv.getTable("inputTable").get();
        System.out.println(iTable);
        
        String stringSQL = new String(Files.readAllBytes(Paths.get("test.sql")));
        tEnv.sqlUpdate(stringSQL);
        
        System.out.println("Stream is being initialized. Execution plan: \n"+ sEnv.getExecutionPlan());
        
        // Blocking until cancellation
        sEnv.execute();
        System.out.println("Stream END");
    }

}
