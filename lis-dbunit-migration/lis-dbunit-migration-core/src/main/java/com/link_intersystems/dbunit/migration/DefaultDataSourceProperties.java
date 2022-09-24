package com.link_intersystems.dbunit.migration;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSourceProperties extends AbstractMap<String, String> implements DataSourceProperties {

    private Map<String, String> environmentProperties = new HashMap<>();
    private Map<String, String> dataSourceProperties = new HashMap<>();

    @Override
    public Map<String, String> getEnvironmentProperties() {
        return environmentProperties;
    }

    public void setEnvironmentProperties(Map<String, String> environmentProperties) {
        this.environmentProperties = requireNonNull(environmentProperties);
    }

    @Override
    public String put(String key, String value) {
        return dataSourceProperties.put(key, value);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return dataSourceProperties.entrySet();
    }
}
