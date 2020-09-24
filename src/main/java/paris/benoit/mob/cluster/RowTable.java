package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

public abstract class RowTable {

    protected String[] fieldNames;
    protected DataType[] fieldTypes;
    protected String name;

    TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    TypeInformation<Row> getReturnType() {
        return (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

}
