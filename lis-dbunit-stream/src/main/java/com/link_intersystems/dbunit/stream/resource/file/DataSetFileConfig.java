package com.link_intersystems.dbunit.stream.resource.file;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.IdentityHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileConfig {

    public static final DataSetFileConfig.ConfigProperty<Charset> CHARSET_PROPERTY = new DataSetFileConfig.ConfigProperty<>(Charset.class, StandardCharsets.UTF_8);

    public static class ConfigProperty<T> {

        private Class<T> propertyType;
        private T defaultValue;


        public ConfigProperty(Class<T> configType, T defaultValue) {
            this.defaultValue = defaultValue;
            this.propertyType = requireNonNull(configType);
        }

        public ConfigProperty(Class<T> propertyType) {
            this.propertyType = requireNonNull(propertyType);
        }

        public T getDefaultValue() {
            return defaultValue;
        }
    }

    public static class ConfigPropertyValue<T> {

        private ConfigProperty<T> property;
        private T propertyValue;

        public ConfigPropertyValue(ConfigProperty<T> property) {
            this.property = requireNonNull(property);
        }

        public void setValue(T propertyValue) {
            this.propertyValue = propertyValue;
        }

        public T getValue() {
            return propertyValue;
        }
    }

    private Map<ConfigProperty, ConfigPropertyValue> propertyValues = new IdentityHashMap<>();

    public <T> void setProperty(ConfigProperty<T> property, T value) {
        ConfigPropertyValue<T> propertyValue = propertyValues.computeIfAbsent(property, p -> new ConfigPropertyValue(p));
        propertyValue.setValue(value);
    }

    public <T> T getProperty(ConfigProperty<T> property) {
        ConfigPropertyValue<T> propertyValue = propertyValues.get(property);

        if (propertyValue == null) {
            return property.getDefaultValue();
        }

        return propertyValue.getValue();
    }

}
