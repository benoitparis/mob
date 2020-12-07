package paris.benoit.mob.cluster;

import java.util.Map;

public class MobTableConfiguration {

    public final String tableName;
    public final String serdeSchema;
    public final Map<String, String> mobOptions;

    public MobTableConfiguration(String tableName, String serdeSchema, Map<String, String> mobOptions) {
        this.tableName = tableName;
        this.serdeSchema = serdeSchema;
        this.mobOptions = mobOptions;
    }

}
