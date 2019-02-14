package paris.benoit.mob.cluster.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import paris.benoit.mob.cluster.MobClusterConfiguration.ConfigurationItem;
import paris.benoit.mob.cluster.MobClusterRegistry;
import paris.benoit.mob.cluster.table.json.JsonTableSource;

public class AppendStreamTableUtils {

    public static JsonTableSource createAndRegisterTableSource(StreamTableEnvironment tEnv, ConfigurationItem inSchema) {
        final JsonTableSource tableSource = new JsonTableSource(inSchema.content);
        MobClusterRegistry.jrds = tableSource.getJsonRowDeserializationSchema();
        tEnv.registerTableSource(inSchema.name + "_raw", tableSource);
        Table hashInputTable = tEnv.sqlQuery(
            "SELECT\n" + 
            "  " + StringUtils.join(tableSource.getTableSchema().getFieldNames(), ",\n  ") + "\n" +
            "FROM " + inSchema.name + "_raw" + " \n"
        );
        DataStream<Row> appendStream = tEnv
            .toAppendStream(hashInputTable, tableSource.getReturnType());
        tEnv.registerTable(inSchema.name, tEnv.fromDataStream(appendStream, 
            StringUtils.join(tableSource.getTableSchema().getFieldNames(), ", ") +
            ", proctime.proctime")
        );
        return tableSource;
    }

}
