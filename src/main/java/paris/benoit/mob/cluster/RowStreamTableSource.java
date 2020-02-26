package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public abstract class RowStreamTableSource implements StreamTableSource<Row> {

    protected String[] fieldNames;
    protected DataType[] fieldTypes;
    protected RichParallelSourceFunction<Row> function;
    protected String name;

    @Override
    public DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    // TODO faudra virer ça quand ils seront prêt pour les types
    @Override
    public TypeInformation<Row> getReturnType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    @Override
    public String explainSource() {
        return name;
    }

}
