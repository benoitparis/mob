package paris.benoit.mob.cluster.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.json.JsonTableSource;

public class AppendStreamTableUtils {

    private static final Logger logger = LoggerFactory.getLogger(AppendStreamTableUtils.class);

    // FIXME
    // Why?
    // Si on crée pas une barrière de matérialization, alors l'engine crée plein d'instances de TableSource
    //   alors qu'on a qu'une fois le message
    //   REM accidentellement, en ayant la redondance avec kafka, ça irait lire aux bons offsets?
    public static JsonTableSource createAndRegisterTableSourceDoMaterializeAsAppendStream(StreamTableEnvironment tEnv, MobTableConfiguration configuration) {
        final JsonTableSource tableSource = new JsonTableSource(configuration);
        tEnv.registerTableSource(configuration.name + "_raw", tableSource);
        logger.info("Registering as TableSource: " + configuration.name + "_raw");
        Table rawTableSource = tEnv.sqlQuery(
                "SELECT\n" +
                        "  " + StringUtils.join(tableSource.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
                        "FROM " + configuration.name + "_raw" + "\n"
        );
        DataStream<Row> appendStream = tEnv
                .toAppendStream(rawTableSource, tableSource.getReturnType());
        logger.info("Registering as Table: " + configuration.name);
        tEnv.registerTable(configuration.name, tEnv.fromDataStream(appendStream,
                StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
                        ", proctime_append_stream.proctime")
        );
        return tableSource;
    }

}