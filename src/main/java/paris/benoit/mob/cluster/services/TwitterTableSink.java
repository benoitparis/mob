package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.types.DataType;
import paris.benoit.mob.cluster.MobTableConfiguration;
import paris.benoit.mob.cluster.TypedRetractStreamTableSink;

public class TwitterTableSink extends TypedRetractStreamTableSink<String> {

    public TwitterTableSink() {
        fieldNames = new String[] { "tweet_text" };
        fieldTypes = new DataType[] { DataTypes.STRING() };
        sinkFunction = new TwitterSinkFunction();
        name = "Twitter Table Sink";
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, String>> ds) {
        return ds
                .addSink(sinkFunction)
                .setParallelism(1)
                .name(name);
    }

    public static void createAndRegister(StreamTableEnvironment tEnv, Catalog catalog, MobTableConfiguration sqlConf) throws TableAlreadyExistException, DatabaseNotExistException {
        ObjectPath twitterPath = new ObjectPath("app_service", "twitter");

        catalog.createTable(
                twitterPath,
                ConnectorCatalogTable.sink(new TwitterTableSink(), false),
                false
        );
        tEnv.insertInto(twitterPath.getFullName(), tEnv.sqlQuery(sqlConf.content));
    }

}
