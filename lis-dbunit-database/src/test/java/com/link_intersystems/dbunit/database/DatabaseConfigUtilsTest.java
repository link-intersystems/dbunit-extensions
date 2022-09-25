package com.link_intersystems.dbunit.database;

import org.dbunit.database.CachedResultSetTableFactory;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DefaultMetadataHandler;
import org.dbunit.database.statement.StatementFactory;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.dbunit.database.DatabaseConfig.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SuppressWarnings("deprecation")
class DatabaseConfigUtilsTest {

    @Test
    void copy() {
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setFeature(FEATURE_BATCHED_STATEMENTS, true);
        databaseConfig.setFeature(FEATURE_QUALIFIED_TABLE_NAMES, true);
        databaseConfig.setFeature(FEATURE_CASE_SENSITIVE_TABLE_NAMES, true);
        databaseConfig.setFeature(FEATURE_DATATYPE_WARNING, true);
        databaseConfig.setFeature(FEATURE_ALLOW_EMPTY_FIELDS, true);

        databaseConfig.setProperty(PROPERTY_STATEMENT_FACTORY, new StatementFactory());
        databaseConfig.setProperty(PROPERTY_RESULTSET_TABLE_FACTORY, new CachedResultSetTableFactory());
        databaseConfig.setProperty(PROPERTY_DATATYPE_FACTORY, new PostgresqlDataTypeFactory());
        databaseConfig.setProperty(PROPERTY_ESCAPE_PATTERN, "\\");
        databaseConfig.setProperty(PROPERTY_TABLE_TYPE, new String[]{"TABLE"});
        databaseConfig.setProperty(PROPERTY_BATCH_SIZE, 100);
        databaseConfig.setProperty(PROPERTY_FETCH_SIZE, 50);
        databaseConfig.setProperty(PROPERTY_METADATA_HANDLER, new DefaultMetadataHandler());
        databaseConfig.setProperty(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH, true);

        DatabaseConfig databaseConfigCopy = DatabaseConfigUtils.copy(databaseConfig);

        assertEquals(databaseConfig, databaseConfigCopy);
    }

    private void assertEquals(DatabaseConfig expected, DatabaseConfig actual) {
        for (String featureName : DatabaseConfigUtils.DATABASE_CONFIG_FEATURE_NAMES) {
            Assertions.assertEquals(expected.getFeature(featureName), actual.getFeature(featureName), "feature: " + featureName);
        }

        for (String propertyName : DatabaseConfigUtils.DATABASE_CONFIG_PROPERTY_NAMES) {
            Assertions.assertEquals(expected.getProperty(propertyName), actual.getProperty(propertyName), "property: " + propertyName);
        }
    }
}