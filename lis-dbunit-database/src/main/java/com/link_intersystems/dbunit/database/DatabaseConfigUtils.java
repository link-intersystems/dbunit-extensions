package com.link_intersystems.dbunit.database;

import org.dbunit.database.DatabaseConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.dbunit.database.DatabaseConfig.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseConfigUtils {

    public static final List<String> DATABASE_CONFIG_FEATURE_NAMES;

    public static final List<String> DATABASE_CONFIG_PROPERTY_NAMES;


    static {
        @SuppressWarnings("deprecation") // unless setFeature is removed from the DatabaseConfig api.
        String[] allFeatures = ALL_FEATURES;
        DATABASE_CONFIG_FEATURE_NAMES = Collections.unmodifiableList(Arrays.asList(allFeatures));

        Predicate<String> onlyProperties = not(DATABASE_CONFIG_FEATURE_NAMES::contains);

        DATABASE_CONFIG_PROPERTY_NAMES = Collections.unmodifiableList(
                Arrays.stream(ALL_PROPERTIES)
                        .map(ConfigProperty::getProperty)
                        .filter(onlyProperties)
                        .collect(Collectors.toList())
        );
    }

    @SuppressWarnings("deprecation") // unless setFeature is removed from the DatabaseConfig api.
    public static void copy(DatabaseConfig sourceDatabaseConfig, DatabaseConfig targetDatabaseConfig) {
        for (String featureName : DATABASE_CONFIG_FEATURE_NAMES) {
            boolean enabled = sourceDatabaseConfig.getFeature(featureName);
            targetDatabaseConfig.setFeature(featureName, enabled);
        }

        for (String propertyName : DATABASE_CONFIG_PROPERTY_NAMES) {
            Object propertyValue = sourceDatabaseConfig.getProperty(propertyName);
            targetDatabaseConfig.setProperty(propertyName, propertyValue);
        }
    }

    private static Predicate<String> not(Predicate<String> predicate) {
        return predicate.negate();
    }

    public static DatabaseConfig copy(DatabaseConfig databaseConfig) {
        DatabaseConfig databaseConfigCopy = new DatabaseConfig();
        copy(databaseConfig, databaseConfigCopy);
        return databaseConfigCopy;
    }
}
