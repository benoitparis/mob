package paris.benoit.mob.cluster;

import java.io.Serializable;

public class MobTableConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    public String content;
    public String name;
    public MobTableConfiguration(String content, String name) {
        super();
        this.content = content;
        this.name = name;
    }
    @Override
    public String toString() {
        return "ConfigurationItem [content=" + content + ", name=" + name + "]";
    }
}