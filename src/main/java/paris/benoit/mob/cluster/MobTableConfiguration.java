package paris.benoit.mob.cluster;

import java.io.IOException;

public class MobTableConfiguration {

    public final String content;

    public MobTableConfiguration(String content) throws IOException {
        this.content = content;
    }

    @Override
    public String toString() {
        return "MobTableConfiguration{" +
                ", content='\n" + content + '\'' +
                '}';
    }

}