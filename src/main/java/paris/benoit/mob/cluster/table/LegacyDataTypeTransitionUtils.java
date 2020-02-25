package paris.benoit.mob.cluster.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LegacyDataTypeTransitionUtils {


    public static DataType convertDataTypeRemoveLegacy(DataType currentType) {

        DataType result = currentType;

        if (currentType instanceof FieldsDataType) {
            FieldsDataType casted = (FieldsDataType) currentType;

            Map<String, DataType> fieldDataTypes = casted.getFieldDataTypes()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            it -> it.getKey(),
                            it -> convertDataTypeRemoveLegacy(it.getValue())
                    ));

            result = new FieldsDataType(convertLogicalTypeRemoveLegacy(casted.getLogicalType()), fieldDataTypes);

        } else if (currentType instanceof AtomicDataType) {
            AtomicDataType casted = (AtomicDataType) currentType;
            if (casted.getLogicalType() instanceof LegacyTypeInformationType) {
                LegacyTypeInformationType logicalCasted = (LegacyTypeInformationType) casted.getLogicalType();
                if (logicalCasted.getTypeRoot().name().equals("DECIMAL")) {
                    result = DataTypes.DECIMAL(38, 18);
                }
            }
        }
        return result;

    }

    public static LogicalType convertLogicalTypeRemoveLegacy(LogicalType currentType) {

        LogicalType result = currentType;
        if (currentType instanceof RowType) {
            RowType casted = (RowType) currentType;
            List<RowType.RowField> converted = casted
                    .getFields()
                    .stream()
                    .map(it -> new RowType.RowField(it.getName(), convertLogicalTypeRemoveLegacy(it.getType())))
                    .collect(Collectors.toList());
            result = new RowType(converted);
        } else if (currentType instanceof LegacyTypeInformationType) {
            LegacyTypeInformationType casted = (LegacyTypeInformationType) currentType;
            if (casted.getTypeRoot().name().equals("DECIMAL")) {
                result = new DecimalType(38, 18);
            }
        }
        return result;
    }

}
