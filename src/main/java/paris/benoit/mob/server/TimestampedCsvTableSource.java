package paris.benoit.mob.server;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.DefinedProctimeAttribute;

public class TimestampedCsvTableSource extends CsvTableSource implements DefinedProctimeAttribute {

    public TimestampedCsvTableSource(String path, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        super(path, fieldNames, fieldTypes);
    }

    @Override
    public String getProctimeAttribute() {
        return "rowtime";
    }

}
