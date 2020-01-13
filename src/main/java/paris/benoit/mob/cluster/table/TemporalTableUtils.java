package paris.benoit.mob.cluster.table;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TemporalTableUtils {
    
    private static final String TEMPORAL_TABLE_PATTERN_REGEX = "CREATE TEMPORAL TABLE FUNCTION ([^ ]+) TIME ATTRIBUTE ([^ ]+) PRIMARY KEY ([^ ]+) AS\\s+(TABLE_SCAN ([^ ]+))?(.*)";
    private static final Pattern TEMPORAL_TABLE_PATTERN = Pattern.compile(TEMPORAL_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    public static void createAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration state) {
        Matcher m = TEMPORAL_TABLE_PATTERN.matcher(state.content);
        
        if (m.matches()) {
            Table historyTable;
            if (null != m.group(4) && m.group(4).trim().startsWith("TABLE_SCAN")) {
                historyTable = tEnv.scan(m.group(5).trim());
            } else {
                historyTable = tEnv.sqlQuery(m.group(6));
            }
            TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction(m.group(2), m.group(3));
            tEnv.registerFunction(m.group(1), temporalTable);
        } else {
            throw new RuntimeException("Failed to create temporal table. They must conform to: " + TEMPORAL_TABLE_PATTERN_REGEX + "\nSQL was: \n" + state);
        }
        
        if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
            throw new RuntimeException("Created table must match with file name");
        }
    }

}
