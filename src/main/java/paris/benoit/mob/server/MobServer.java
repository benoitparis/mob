package paris.benoit.mob.server;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import paris.benoit.mob.test.AppTestSuiteRunner;

import java.util.Arrays;

import static paris.benoit.mob.cluster.utils.Colors.green;

class MobServer {
    // Do not move further down, quasar instrumentation can fail depending on class initialization sequence
    static {
        if(getVersion() != 8) {
            System.out.println("Error: A Java 8 runtime must be used");
            System.out.println("The maven exec:exec goal can take an executable path with: -Djava.executable=path/to/java");
            System.exit(-2);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(MobServer.class);

    public static void main(String[] args) throws Exception {

        logger.info(green(
            "\n" + " ::::    ::::   ::::::::  :::::::::  :::        ::::::::::: :::::::::  " +
            "\n" + " +:+:+: :+:+:+ :+:    :+: :+:    :+: :+:            :+:     :+:    :+: " +
            "\n" + " +:+ +:+:+ +:+ +:+    +:+ +:+    +:+ +:+            +:+     +:+    +:+ " +
            "\n" + " +#+  +:+  +#+ +#+    +:+ +#++:++#+  +#+            +#+     +#++:++#+  " +
            "\n" + " +#+       +#+ +#+    +#+ +#+    +#+ +#+            +#+     +#+    +#+ " +
            "\n" + " #+#       #+# #+#    #+# #+#    #+# #+#            #+#     #+#    #+# " +
            "\n" + " ###       ###  ########  #########  ########## ########### #########  " +
        ""));

        final CommandLineParser parser = new DefaultParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(cliOptions(), args);
        } catch (MissingOptionException e) {
            System.out.println(e.getMessage());
            new HelpFormatter().printHelp("MobLib", cliOptions(), true);
            System.exit(-1);
        }
        String names = cmdLine.getOptionValue("app-name").trim();

        ClusterRunner runner;

        if (cmdLine.hasOption("test-suite")) {
            runner = new AppTestSuiteRunner();
        } else {
            runner = new ServerRunner();
        }

        logger.info(green("Launching " + names));
        runner.run(Arrays.asList(names.split(",")));
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

        final Option runTests = Option.builder("t")
                .longOpt("test-suite") //
                .desc("Run the test suite of an app")
                .build();

        final Options options = new Options();
        options.addOption(appName);
        options.addOption(runTests);
        return options;
    }
}
