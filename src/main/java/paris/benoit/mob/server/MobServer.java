package paris.benoit.mob.server;

import org.apache.commons.cli.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.cluster.MobClusterConfiguration;
import paris.benoit.mob.cluster.MobClusterRegistry;

public class MobServer {
    private static final Logger logger = LoggerFactory.getLogger(MobServer.class);

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";

    public static void main(String[] args) throws Exception {

        logger.info("\n" + ANSI_GREEN +
            "::::    ::::   ::::::::  :::::::::  :::        ::::::::::: :::::::::  \n" +
            "+:+:+: :+:+:+ :+:    :+: :+:    :+: :+:            :+:     :+:    :+: \n" +
            "+:+ +:+:+ +:+ +:+    +:+ +:+    +:+ +:+            +:+     +:+    +:+ \n" +
            "+#+  +:+  +#+ +#+    +:+ +#++:++#+  +#+            +#+     +#++:++#+  \n" +
            "+#+       +#+ +#+    +#+ +#+    +#+ +#+            +#+     +#+    +#+ \n" +
            "#+#       #+# #+#    #+# #+#    #+# #+#            #+#     #+#    #+# \n" +
            "###       ###  ########  #########  ########## ########### #########  \n" +
        ANSI_RESET);

        final CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(cliOptions(), args);
        } catch (MissingOptionException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp("MobLib", cliOptions(), true);
            System.exit(-1);
        }
        String name = cmdLine.getOptionValue("app-name").trim();

        if(getVersion() != 8) {
            System.out.println("Error: A Java 8 runtime must be used");
            System.out.println("The maven exec:exec goal can take an executable path with: -Djava.executable=path/to/java");
            System.exit(-2);
        }

        logger.info(ANSI_GREEN + "Launching " + name + ANSI_RESET);
        launchApp(name);
    }

    public final static int DEFAULT_STREAM_PARALLELISM = 4;
    // Apparamment à 1ms on est seulement 25% en dessous du max
    // https://flink.apache.org/2019/06/05/flink-network-stack.html
    public final static int DEFAULT_MAX_BUFFER_TIME_MILLIS = 5;
    public final static int DEFAULT_FRONT_PORT = 8090;
    public final static int DEFAULT_FLINK_WEB_UI_PORT = 8082;
    
    public static void launchApp(String appName) throws Exception {

        MobClusterConfiguration configuration = new MobClusterConfiguration(
            appName,
            new UnderTowLauncher(DEFAULT_FRONT_PORT),
            TimeCharacteristic.IngestionTime,
            DEFAULT_STREAM_PARALLELISM,
            DEFAULT_MAX_BUFFER_TIME_MILLIS,
            DEFAULT_FLINK_WEB_UI_PORT
        );
        MobClusterRegistry registry = new MobClusterRegistry(configuration);
        
        registry.start();
        
    }

    private static int getVersion() {
        String version = System.getProperty("java.version");
        System.out.println("Java version: " + version);
        if(version.startsWith("1.")) {
            version = version.substring(2, 3);
        } else {
            int dot = version.indexOf(".");
            if(dot != -1) { version = version.substring(0, dot); }
        } return Integer.parseInt(version);
    }

    private static Options cliOptions() {

        final Option appName = Option.builder("a")
                .longOpt("app-name") //
                .desc("The app name, located under apps/")
                .hasArg(true)
                .required(true)
                .build();

        final Options options = new Options();
        options.addOption(appName);
        return options;
    }
}
