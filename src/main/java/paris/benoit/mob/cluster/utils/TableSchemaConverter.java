package paris.benoit.mob.cluster.utils;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.*;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TableSchemaConverter {

    // TODO mettre des tests
    public static String toJsonSchema(TableSchema schema) {

        String schemaString = "";
        schemaString +=
                "{\n" +
                "  \"$schema\": \"http://json-schema.org/draft-04/schema#\",\n" +
                "  \"type\": \"object\",\n" +
                "  \"properties\": {\n";
        String properties = schema.getTableColumns().stream()
                .map(it -> new NameTypePair(it.getName(), it.getType().getLogicalType()))
                .map(it -> getJsonPropertyStringLeaf(it, 2))
                .collect(Collectors.joining(",\n"));
        schemaString += properties;
        schemaString +=
                "\n" +
                "  }\n" +
                "}";
        return schemaString;

    }

    static StringBuffer getJsonPropertyStringLeaf(NameTypePair pair, int level) {
        if (pair.type instanceof VarCharType) {
            return formatProp(pair, "string", Collections.emptyList(), level);
        } else if (pair.type instanceof TimestampType) {
            return formatProp(pair, "string", Collections.emptyList(), level);
        } else if (pair.type instanceof BigIntType) {
            return formatProp(pair, "number", Collections.emptyList(), level);
        } else if (pair.type instanceof RowType) {
            RowType logicalTypeCasted = (RowType) pair.type;
            List<NameTypePair> list = logicalTypeCasted.getFields().stream()
                    .map(it -> new NameTypePair(it.getName(), it.getType()))
                    .collect(Collectors.toList()
            );
            return formatProp(pair, "object", list, level);
        }
        throw new RuntimeException("Unknown type: " + pair);
    }

    private static StringBuffer formatProp(NameTypePair pair, String type, List<NameTypePair> list, int level) {
        StringBuffer result = new StringBuffer()
            .append("  ".repeat(level))
            .append('"' + pair.name + '"' + ": {" + '\n')
            .append("  ".repeat(level))
            .append("  \"type\": \"" + type + '"' + '\n');
        if (list.size() > 0) {
            result
                .append("  ".repeat(level))
                .append("  \"properties\": {\n");
            result.append(
                list.stream()
                    .map(it -> getJsonPropertyStringLeaf(it, level + 2))
                    .collect(Collectors.joining(",\n", "", "\n"))
            );
            result
                .append("  ".repeat(level))
                .append("  }\n");
        }
        result
            .append("  ".repeat(level))
            .append("}");
        return result;

    }

    static class NameTypePair {
        String name;
        LogicalType type;
        public NameTypePair(String name, LogicalType type) {
            this.name = name;
            this.type = type;
        }
        @Override
        public String toString() {
            return "NameTypePair{" +
                    "name='" + name + '\'' +
                    ", type=" + type +
                    ", LogicalType: " + type.getClass() +
                    '}';
        }
    }


}
