package paris.benoit.mob.cluster;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    public String ddl;
    public String name;
    public MobTableConfiguration(String ddl, String name) {
        super();
        this.ddl = ddl;
        this.name = name;
    }
    @Override
    public String toString() {
        return "ConfigurationItem [ddl=" + ddl + ", name=" + name + "]";
    }
}