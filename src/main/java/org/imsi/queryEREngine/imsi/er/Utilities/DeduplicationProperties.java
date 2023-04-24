package org.imsi.queryEREngine.imsi.er.Utilities;

import org.imsi.queryEREngine.imsi.calcite.util.DeduplicationExecution;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DeduplicationProperties {

    private static final String pathToPropertiesFile = "deduplication.properties";
    private static final String BP = "mb.bp";
    private static final String BF = "mb.bf";
    private static final String EP = "mb.ep";
    private static final String LINKS = "links";
    private static final String JOIN = "join";
    private static final String FILTER_PARAM = "filter.param";

    private static boolean runBP = true;
    private static boolean runBF = true;
    private static boolean runEP = true;
    private static boolean runLinks = true;
    private static boolean runAES = true;
    private static double filterParam = 0.0;

    private static Properties properties;

    public DeduplicationProperties() {
        super();
        properties = loadProperties();
        if (!properties.isEmpty()) {
            runBP = Boolean.parseBoolean(properties.getProperty(BP));
            runBF = Boolean.parseBoolean(properties.getProperty(BF));
            runEP = Boolean.parseBoolean(properties.getProperty(EP));
            runLinks = Boolean.parseBoolean(properties.getProperty(LINKS));
            runAES = Boolean.parseBoolean(properties.getProperty(JOIN));
            filterParam = Double.parseDouble(properties.getProperty(FILTER_PARAM));
        }
    }

    private static Properties loadProperties() {

        Properties prop = new Properties();
        try (InputStream input = DeduplicationExecution.class.getClassLoader().getResourceAsStream(pathToPropertiesFile)) {
            // load a properties file
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return prop;
    }

    public static boolean isRunBP() {
        return runBP;
    }

    public static boolean isRunBF() {
        return runBF;
    }

    public static boolean isRunEP() {
        return runEP;
    }

    public static boolean isRunLinks() {
        return runLinks;
    }

    public static double getFilterParam() {
        return filterParam;
    }

    public static Properties getProperties() {
        return properties;
    }

    public static boolean isRunAES() {
        return runAES;
    }
}
