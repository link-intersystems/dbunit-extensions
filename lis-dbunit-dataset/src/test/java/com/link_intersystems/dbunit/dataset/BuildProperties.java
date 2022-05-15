package com.link_intersystems.dbunit.dataset;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BuildProperties {

    private final Properties properties;

    public BuildProperties() {
        properties = new Properties();
        try {
            properties.load(BuildProperties.class.getResourceAsStream("/maven.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public File getBuildOutputDirectory() {
        return new File(properties.getProperty("targetDir"));
    }
}
