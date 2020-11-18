package paris.benoit.mob.cluster.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalTableFunctionUtils {
    
    private static final String TEMPORAL_TABLE_PATTERN_REGEX = "CREATE TEMPORAL TABLE FUNCTION ([^ ]+) TIME ATTRIBUTE ([^ ]+) PRIMARY KEY ([^ ]+) AS\\s+(.*)";
    private static final Pattern TEMPORAL_TABLE_PATTERN = Pattern.compile(TEMPORAL_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    public static void createAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration state) {
        Matcher m = TEMPORAL_TABLE_PATTERN.matcher(state.content);

        QueryOperationCatalogViewTable
        
        if (m.matches()) {
            Table historyTable = tEnv.sqlQuery(m.group(4));

            TemporalTableFunction temporalTable = historyTable.createTemporalTableFunction($(m.group(2)), $(m.group(3)));
            tEnv.registerFunction(m.group(1), temporalTable);
        } else {
            throw new RuntimeException("Failed to create temporal table. They must conform to: " + TEMPORAL_TABLE_PATTERN_REGEX + "\nSQL was: \n" + state);
        }
        
        if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
            throw new RuntimeException("Created table must match with file name");
        }
    }

}
