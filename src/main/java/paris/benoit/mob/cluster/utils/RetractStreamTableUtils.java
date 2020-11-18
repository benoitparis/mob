package paris.benoit.mob.cluster.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

public class RetractStreamTableUtils {

    private static final String RETRACT_TABLE_PATTERN_REGEX = "CREATE TABLE ([^ ]+) AS\\s+CONVERT ([^ ]+) TO RETRACT STREAM(.*)";
    private static final Pattern RETRACT_TABLE_PATTERN = Pattern.compile(RETRACT_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    // TODO mettre dans les properties, mapper sur les yaml du client SQL
    public static void convertAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration state) {
        Matcher m = RETRACT_TABLE_PATTERN.matcher(state.content);

        if (m.matches()) {
            System.out.println(m.group(2));

            String fromTableName = m.group(2);

            Table fromTable = tEnv.from(fromTableName);
            DataStream<Tuple2<Boolean, Row>> retractStream = tEnv.toRetractStream(fromTable, Row.class);
            Table retractTable = tEnv.fromDataStream(retractStream, $("accumulate_flag"), $("content"));
            tEnv.createTemporaryView(state.getObjectPath().getFullName(), retractTable);

        } else {
            throw new RuntimeException("Failed to convert to retract stream. Expression must conform to: " + RETRACT_TABLE_PATTERN_REGEX + "\nSQL was: \n" + state);
        }

        if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
            throw new RuntimeException("Created table must match with file name");
        }
    }
}
