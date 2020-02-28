package paris.benoit.mob.cluster;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

public abstract class TypedTable<T> {

    protected String[] fieldNames;
    protected DataType[] fieldTypes;
    protected String name;

    TableSchema getTableSchema() {
        return TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    DataType getProducedDataType() {
        return getTableSchema().toRowDataType();
    }

    TypeInformation<T> getReturnType() {
        return (TypeInformation<T>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
    }

}
