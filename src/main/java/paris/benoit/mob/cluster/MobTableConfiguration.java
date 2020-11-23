package paris.benoit.mob.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MobTableConfiguration implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(MobTableConfiguration.class);

    // REM on chasse les enum de conf et ils reviennent à grand pas
    public static final String MOB_TABLE_NAME = "mob.table-name"; // fully qualifed name
    public static final String MOB_CLUSTER_IO_FLOW = "mob.cluster-io.flow"; // in, out
    public static final String MOB_CLUSTER_IO_TYPE = "mob.cluster-io.type"; // client, js-engine, service
    public static final String MOB_CLUSTER_IO_JS_ENGINE_CODE = "mob.js-engine.code"; // location of file containting the code
    public static final String MOB_CLUSTER_IO_JS_ENGINE_INVOKE_FUNCTION = "mob.js-engine.invoke-function"; // location of file containing the code

    public static final Map<String, String> DEFAULT_CONFIGURATION = new HashMap<>();
    static {
        DEFAULT_CONFIGURATION.put(MobTableConfiguration.MOB_CLUSTER_IO_TYPE, "client");
    }

    public final String dbName;
    public final String content;

    // TODO interpret avec Properties, store as HashMap (pour le masking des defaults)
    public final Properties properties;

    public MobTableConfiguration(String dbName, String content) throws IOException {
        super();
        this.content = content;
        this.dbName = dbName;
        this.properties = buildProperties(content);
    }


    // TODO faire par substring plutôt que regexes
    private static final String COMMENT_CONFIGURATION_REGEX = "/\\*\\s*\\+\\s*moblib\\s*\\(\\s*([^\\)]+)\\)\\s*\\*/(.*)";
    private static final Pattern COMMENT_CONFIGURATION_PATTERN = Pattern.compile(COMMENT_CONFIGURATION_REGEX, Pattern.DOTALL);
    private Properties buildProperties(String content) throws IOException {

        Matcher m = COMMENT_CONFIGURATION_PATTERN.matcher(content);

        if (m.matches() && (null != m.group(1))) {
            Properties props = new Properties();
            props.putAll(MobTableConfiguration.DEFAULT_CONFIGURATION);

            props.load(new StringReader(m.group(1)));
            return props;
        }
        return null;
    }

    @Override
    public String toString() {
        return "MobTableConfiguration{" +
                "dbName='" + dbName + '\'' +
                ", content='\n" + content + '\'' +
                '}';
    }

}