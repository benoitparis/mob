package paris.benoit.mob.cluster;

import org.apache.flink.table.catalog.ObjectPath;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    public enum CONF_TYPE {
        TABLE // TODO rename en VIEW? -> faire avec passage en yaml
        , STATE, UPDATE, JS_ENGINE, RETRACT, APPEND,
        // TODO: use?
        IN_JSONSCHEMA, OUT_JSONSCHEMA
    }

    public final String dbName;
    public final String name;
    final CONF_TYPE confType;
    public final String content;

    public MobTableConfiguration(String dbName, String name, String content, CONF_TYPE confType) {
        super();
        this.content = content;
        this.dbName = dbName;
        this.name = name;
        this.confType = confType;
    }

    @Override
    public String toString() {
        return "MobTableConfiguration{" +
                "dbName='" + dbName + '\'' +
                ", name='" + name + '\'' +
                ", confType=" + confType +
                ", content='\n" + content + '\'' +
                '}';
    }

    public String fullyQualifiedName() {
        return getObjectPath().getFullName();
    }

    public ObjectPath getObjectPath() {
        return new ObjectPath(dbName, name);
    }
}