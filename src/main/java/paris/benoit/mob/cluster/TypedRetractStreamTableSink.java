package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;

public abstract class TypedRetractStreamTableSink<T> extends TypedTable<T> implements RetractStreamTableSink<T> {

    protected RichSinkFunction<Tuple2<Boolean, T>> sinkFunction;

    @Override
    public TableSink<Tuple2<Boolean, T>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException(
                "TypedRetractStreamTableSink classes are to be configured through their constructors"
        );
    }

    @Override
    public TableSchema getTableSchema() {
        return super.getTableSchema();
    }

    @Override
    public TypeInformation<T> getRecordType() {
        return super.getReturnType();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, T>> ds) {
        consumeDataStream(ds);
    }
}
