package paris.benoit.mob.cluster.table.debug;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugTableSink implements RetractStreamTableSink<String> {
    private static final Logger logger = LoggerFactory.getLogger(DebugTableSink.class);

    private static final String[] fieldNames = new String[] { "debug_info" };
    private DataType[] fieldTypes = new DataType[] { DataTypes.STRING() };

    private RichSinkFunction<Tuple2<Boolean, String>> function;

    public DebugTableSink() {
        function = new RichSinkFunction<Tuple2<Boolean, String>>() {
            @Override
            public void invoke(Tuple2<Boolean, String> value, Context context) throws Exception {
                System.out.println(value);
            }
        };
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public TableSink<Tuple2<Boolean, String>> configure(String[] strings, TypeInformation<?>[] typeInformations) {
        throw new UnsupportedOperationException("Moblib: This class is configured through its constructor");
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, String>> ds) {
        consumeDataStream(ds);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, String>> ds) {
        return ds
                .addSink(function)
                .name("Debug DataStreamSink");
    }

    // TODO enlever quand ils seront prêt
    @Override
    public TypeInformation<String> getRecordType() {
        return (TypeInformation<String>) TypeConversions.fromDataTypeToLegacyInfo(this.getTableSchema().toRowDataType());
    }

    public String getName() {
        return "Debug TableSink";
    }

}