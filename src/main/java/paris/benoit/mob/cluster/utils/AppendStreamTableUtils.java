package paris.benoit.mob.cluster.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobTableConfiguration;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AppendStreamTableUtils {

    private static final Logger logger = LoggerFactory.getLogger(AppendStreamTableUtils.class);

    private static final String APPEND_TABLE_PATTERN_REGEX = "CREATE TABLE ([^ ]+) AS\\s+CONVERT ([^ ]+) TO APPEND STREAM(.*)";
    private static final Pattern APPEND_TABLE_PATTERN = Pattern.compile(APPEND_TABLE_PATTERN_REGEX, Pattern.DOTALL);

    // TODO remove au profit de kafka-upsert, et d'un éventuel EMIT CHANGESTREAM
    public static void convertAndRegister(StreamTableEnvironment tEnv, MobTableConfiguration state) {
        Matcher m = APPEND_TABLE_PATTERN.matcher(state.content);

        if (m.matches()) {
            String fromTableName = m.group(2);

            Table fromTable = tEnv.from(fromTableName);

            DataStream<Row> filteredRetractStream = tEnv
                    .toRetractStream(fromTable, fromTable.getSchema().toRowType())
//                    .toRetractStream(fromTable, Row.class) // very weird bug: not called, yet makes the ball flicker
                    .filter(it -> it.f0)
                    .map(it -> it.f1);
            Table retractTable = tEnv.fromDataStream(filteredRetractStream, StringUtils.join(fromTable.getSchema().getFieldNames(), ", ") +
                    ", proctime_append_stream.proctime");
            tEnv.createTemporaryView(state.fullyQualifiedName(), retractTable);

        } else {
            throw new RuntimeException("Failed to convert to retract stream. Expression must conform to: " + APPEND_TABLE_PATTERN_REGEX + "\nSQL was: \n" + state);
        }

        if (!m.group(1).trim().equalsIgnoreCase(state.name.trim())) {
            throw new RuntimeException("Created table must match with file name");
        }
    }

    // FIXME https://issues.apache.org/jira/browse/FLINK-15775
    public static void createAndRegisterTableSourceDoMaterializeAsAppendStream(String appName, StreamTableEnvironment tEnv, Catalog catalog, StreamTableSource tableSource, String name) throws TableAlreadyExistException, DatabaseNotExistException {

        Table rawTable = tEnv.fromTableSource(tableSource);

        DataStream<Row> appendStream = tEnv.toAppendStream(rawTable, Row.class);

        logger.info("Registering as Table: " + appName + "." + name);
        tEnv.createTemporaryView(appName + "." + name, tEnv.fromDataStream(appendStream,
                StringUtils.join(rawTable.getSchema().getFieldNames(), ", ") +
                ", proctime_append_stream.proctime"
            )
        );

    }

}
