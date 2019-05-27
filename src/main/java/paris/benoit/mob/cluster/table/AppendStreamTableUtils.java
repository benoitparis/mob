package paris.benoit.mob.cluster.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.table.json.JsonTableSource;

public class AppendStreamTableUtils {

    public static JsonTableSource createAndRegisterTableSource(StreamTableEnvironment tEnv, MobTableConfiguration configuration) {
        final JsonTableSource tableSource = new JsonTableSource(configuration);
        tEnv.registerTableSource(configuration.name + "_raw", tableSource);
        Table hashInputTable = tEnv.sqlQuery(
            "SELECT\n" + 
            "  " + StringUtils.join(tableSource.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
            "FROM " + configuration.name + "_raw" + "\n"
        );
        DataStream<Row> appendStream = tEnv
            .toAppendStream(hashInputTable, tableSource.getReturnType());
        tEnv.registerTable(configuration.name, tEnv.fromDataStream(appendStream, 
            StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
            ", proctime.proctime")
        );
        return tableSource;
    }

}
