package paris.benoit.mob.cluster;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    public enum CONF_TYPE {
        TABLE, STATE, UPDATE, JS_ENGINE, RETRACT,
        // TODO: use?
        IN_JSONSCHEMA, OUT_JSONSCHEMA
    }

    public String name;
    public String content;
    CONF_TYPE confType;

    public MobTableConfiguration(String name, String content, CONF_TYPE confType) {
        super();
        this.content = content;
        this.name = name;
        this.confType = confType;
    }

    @Override
    public String toString() {
        return "MobTableConfiguration{" +
                "name='" + name + '\'' +
                ", content='" + content + '\'' +
                ", confType=" + confType +
                '}';
    }
}