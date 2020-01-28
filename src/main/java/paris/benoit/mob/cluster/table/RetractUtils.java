package paris.benoit.mob.cluster.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RetractUtils {

    private static final String RETRACT_TABLE_PATTERN_REGEX = "CREATE TABLE ([^ ]+) AS\\s+CONVERT ([^ ]+) TO RETRACT STREAM(.*)";
    private static final Pattern RETRACT_TABLE_PATTERN = Pattern.compile(RETRACT_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    public static void createAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration state) {
        Matcher m = RETRACT_TABLE_PATTERN.matcher(state.content);

        if (m.matches()) {
            String toName = m.group(1);
            String fromTableName = m.group(2);

            Table fromTable = tEnv.sqlQuery("SELECT * FROM " + fromTableName);
            DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(fromTable, fromTable.getSchema().toRowType());
            Table retractTable = tEnv.fromDataStream(retractStream, "flag, content");
            tEnv.registerTable(toName, retractTable);

        } else {
            throw new RuntimeException("Failed to convert to retract stream. Expression must conform to: " + RETRACT_TABLE_PATTERN_REGEX + "\nSQL was: \n" + state);
        }

        if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
            throw new RuntimeException("Created table must match with file name");
        }
    }
}
