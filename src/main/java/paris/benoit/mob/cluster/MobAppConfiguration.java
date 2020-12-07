package paris.benoit.mob.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MobAppConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(MobAppConfiguration.class);

    public final String name;
    public final List<String> sql;
    public final List<String> tests;
    private final String basePath;

    public final List<MobTableConfiguration> tableConfiguration = new ArrayList<>();

    public MobAppConfiguration(String name) {
        this.name = name;
        this.basePath = System.getProperty("user.dir") + "/apps/" + name + "/";
        logger.info("Configuration with basePath:" + basePath);

        this.sql = buildConfigurationItem("sql");
        this.tests = buildConfigurationItem("tests");
    }

    private List<String> buildConfigurationItem(final String folder) throws RuntimeException {

        Spliterator<Path> files;
        try {
            files = Files
                    .newDirectoryStream(Paths.get(basePath).resolve(folder))
                    .spliterator();
        } catch (IOException e) {
            files = Spliterators.emptySpliterator();
        }

        return StreamSupport
                .stream(files, false)
                .filter(it -> !Files.isDirectory(it))
                .sorted(Comparator.comparing(a -> {
                        try {
                            return Integer.valueOf(a.getFileName().toString().split("_")[0]);
                        } catch (NumberFormatException e) {
                            return -1; // Arbitrary order
                        }
                    }
                ))
                .map(it -> {
                    try {
                        return new String(Files.readAllBytes(it));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    public List<String> getTests() {
        return tests;
    }

    public void addTableConfiguration(MobTableConfiguration conf) {
        tableConfiguration.add(conf);
    }


}
