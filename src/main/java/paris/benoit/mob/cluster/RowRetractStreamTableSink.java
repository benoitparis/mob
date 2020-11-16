package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public abstract class RowRetractStreamTableSink extends RowTable implements RetractStreamTableSink<Row> {

    protected RichSinkFunction<Tuple2<Boolean, Row>> sinkFunction;

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        throw new UnsupportedOperationException(
                "TypedRetractStreamTableSink classes are to be configured through their constructors"
        );
    }

    @Override
    public TableSchema getTableSchema() {
        return super.getTableSchema();
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return super.getReturnType();
    }


}
