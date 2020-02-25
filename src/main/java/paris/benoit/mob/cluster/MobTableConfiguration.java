package paris.benoit.mob.cluster;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    public enum CONF_TYPE {
        TABLE // TODO rename en VIEW?
        , STATE, UPDATE, JS_ENGINE, RETRACT, APPEND,
        // TODO: use?
        IN_JSONSCHEMA, OUT_JSONSCHEMA
    }

    public String dbName;
    public String name;
    CONF_TYPE confType;
    public String content;

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
                ", content='" + content + '\'' +
                '}';
    }

    public String fullyQualifiedName() {
        return dbName + "." + name;
    }
}