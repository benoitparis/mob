package paris.benoit.mob.cluster;

import org.apache.flink.table.catalog.ObjectPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MobTableConfiguration implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(MobTableConfiguration.class);

    public enum CONF_TYPE {
        // TODO remove? ou bien adapt avec un pattern de l'action à faire (admin vs client vs js-engine? hey, on a toujours besoin des retracts)
        TABLE // TODO rename en VIEW? -> faire avec passage en yaml? Mettre juste insert ou update, pour suivre l'API Flink?
        , STATE, UPDATE, JS_ENGINE, RETRACT, APPEND
    }

    public final String dbName;
    public final String name;
    final CONF_TYPE confType;
    public final String content;

    // TODO interpret avec Properties, store as HashMap (pour le masking des defaults)
    public final Properties properties;

    public MobTableConfiguration(String dbName, String name, String content, CONF_TYPE confType) throws IOException {
        super();
        this.content = content;
        this.dbName = dbName;
        this.name = name;
        this.confType = confType;
        this.properties = buildProperties(content);
    }

    // TODO faire par substring plutôt que regexes
    private static final String COMMENT_CONFIGURATION_REGEX = "/\\*\\s*\\+\\s*moblib\\s*\\(\\s*([^\\)]+)\\)\\s*\\*/(.*)";
    private static final Pattern COMMENT_CONFIGURATION_PATTERN = Pattern.compile(COMMENT_CONFIGURATION_REGEX, Pattern.DOTALL);
    private Properties buildProperties(String content) throws IOException {

        Matcher m = COMMENT_CONFIGURATION_PATTERN.matcher(content);

        if (m.matches() && (null != m.group(1))) {
            Properties props = new Properties();
            props.load(new StringReader(m.group(1)));
            logger.info("Properties for conf: " + name + ": " + props);
            return props;
        }
        return null;
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