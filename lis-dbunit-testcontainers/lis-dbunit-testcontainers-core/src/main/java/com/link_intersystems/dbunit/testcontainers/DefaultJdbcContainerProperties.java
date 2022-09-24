package com.link_intersystems.dbunit.testcontainers;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultJdbcContainerProperties extends AbstractMap<String, String> implements JdbcContainerProperties {

    private Map<String, String> environmentProperties = new HashMap<>();
    private Map<String, String> containerProperties = new HashMap<>();

    @Override
    public Map<String, String> getEnvironment() {
        return environmentProperties;
    }

    public void setEnvironment(Map<String, String> environmentProperties) {
        this.environmentProperties = requireNonNull(environmentProperties);
    }

    @Override
    public String put(String key, String value) {
        return containerProperties.put(key, value);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return containerProperties.entrySet();
    }
}
