package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public abstract class RowStreamTableSource extends RowTable implements StreamTableSource<Row> {

    protected RichParallelSourceFunction<Row> sourceFunction;

    @Override
    public TableSchema getTableSchema() {
        return super.getTableSchema();
    }

    @Override
    public DataType getProducedDataType() {
        return super.getProducedDataType();
    }

    // TODO remove when they are ready with types
    @Override
    public TypeInformation<Row> getReturnType() {
        return super.getReturnType();
    }

    @Override
    public String explainSource() {
        return name;
    }

}
