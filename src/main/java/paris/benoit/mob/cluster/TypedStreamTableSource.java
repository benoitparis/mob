package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;

public abstract class TypedStreamTableSource<T> extends TypedTable<T> implements StreamTableSource<T> {

    protected RichParallelSourceFunction<T> function;

    @Override
    public TableSchema getTableSchema() {
        return super.getTableSchema();
    }

    @Override
    public DataType getProducedDataType() {
        return super.getProducedDataType();
    }

    @Override
    public TypeInformation<T> getReturnType() {
        return super.getReturnType();
    }

    @Override
    public String explainSource() {
        return name;
    }

}
