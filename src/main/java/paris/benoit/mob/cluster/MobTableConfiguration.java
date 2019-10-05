package paris.benoit.mob.cluster;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    public enum CONF_TYPE {
        TABLE, STATE, UPDATE, JS_ENGINE,
        // TODO: use?
        IN_JSONSCHEMA, OUT_JSONSCHEMA
    }

    private static final long serialVersionUID = 1L;

    public String name;
    public String content;
    public CONF_TYPE confType;

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