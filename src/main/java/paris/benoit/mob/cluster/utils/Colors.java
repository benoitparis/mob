package paris.benoit.mob.cluster.utils;

import java.util.List;

public class Colors {

    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_PURPLE = "\u001B[35m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String ANSI_BRIGHT_BLACK = "\u001B[90m";

    public static String red(String in) {
        return ANSI_RED + in + ANSI_RESET;
    }
    public static String green(String in) {
        return ANSI_GREEN + in + ANSI_RESET;
    }
    public static String yellow(String in) {
        return ANSI_YELLOW + in + ANSI_RESET;
    }
    public static String blue(String in) {
        return ANSI_BLUE + in + ANSI_RESET;
    }
    public static String purple(String in) {
        return ANSI_PURPLE + in + ANSI_RESET;
    }
    public static String cyan(String in) {
        return ANSI_CYAN + in + ANSI_RESET;
    }
    public static String brightBlack(String in) {
        return ANSI_BRIGHT_BLACK + in + ANSI_RESET;
    }

}
