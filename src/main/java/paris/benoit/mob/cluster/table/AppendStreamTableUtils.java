package paris.benoit.mob.cluster.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppendStreamTableUtils {

    private static final Logger logger = LoggerFactory.getLogger(AppendStreamTableUtils.class);

    // FIXME
    // Why?
    // Si on crée pas une barrière de matérialization, alors l'engine crée plein d'instances de TableSource
    //   alors qu'on a qu'une fois le message
    //   REM accidentellement, en ayant la redondance avec kafka, ça irait lire aux bons offsets?
    //   REM si on fixe ça il faudrat mettre des DefinedProctimeAttribute aux sources
    public static void createAndRegisterTableSourceDoMaterializeAsAppendStream(StreamTableEnvironment tEnv, StreamTableSource tableSource, String name) {

        tEnv.registerTableSource(name + "_raw", tableSource);

        Table rawTable = tEnv.fromTableSource(tableSource);
        DataStream<Row> appendStream = tEnv.toAppendStream(rawTable, tableSource.getReturnType());
        logger.info("Registering as Table: " + name);
        tEnv.registerTable(name, tEnv.fromDataStream(appendStream,
                StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
                ", proctime_append_stream.proctime"
            )
        );

    }

}
