package paris.benoit.mob.cluster.services;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import paris.benoit.mob.cluster.RowRetractStreamTableSink;

public class TwitterTableSink extends RowRetractStreamTableSink {

    public TwitterTableSink() {
        fieldNames = new String[] { "tweet_text" };
        fieldTypes = new DataType[] { DataTypes.STRING() };
        sinkFunction = new TwitterSinkFunction();
        name = "Twitter Table Sink";
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> ds) {
        return ds
                .addSink(sinkFunction)
                .setParallelism(1)
                .name(name);
    }

}
