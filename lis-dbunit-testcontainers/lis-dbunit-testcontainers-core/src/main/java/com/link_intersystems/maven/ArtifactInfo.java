package com.link_intersystems.maven;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Properties;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ArtifactInfo {

    private Logger logger = LoggerFactory.getLogger(ArtifactInfo.class);

    private Class<?> artifactClass;

    private Properties properties;

    public ArtifactInfo(Class<?> artifactClass) {
        this.artifactClass = artifactClass;
    }

    private Properties getProperties() {
        if (properties == null) {
            properties = new Properties();
            ProtectionDomain protectionDomain = artifactClass.getProtectionDomain();
            CodeSource codeSource = protectionDomain.getCodeSource();
            if (codeSource != null) {
                URL location = codeSource.getLocation();
                URLClassLoader urlClassLoader = new URLClassLoader(new URL[]{location});
                URL pomProperties = urlClassLoader.getResource("META-INF/maven/pom.properties");
                try {
                    try (InputStream in = pomProperties.openStream()) {
                        properties.load(in);
                    }
                } catch (IOException e) {
                    logger.warn("ArtifactInfo not available {}", artifactClass, e);
                }
            }
        }
        return properties;
    }

    public String getGroupId() {
        return getProperties().getProperty("groupId");
    }

}
