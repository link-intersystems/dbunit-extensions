package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;
import static org.dbunit.database.DatabaseConfig.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DBunitJdbcContainer {


    private final JdbcDatabaseContainer<?> jdbcDatabaseContainer;
    private DatabaseConfig dbunitConfig;

    public DBunitJdbcContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer) {
        this(jdbcDatabaseContainer, new DatabaseConfig());
    }

    public DBunitJdbcContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer, DatabaseConfig dbunitConfig) {
        this.jdbcDatabaseContainer = requireNonNull(jdbcDatabaseContainer);
        this.dbunitConfig = requireNonNull(dbunitConfig);
    }

    public RunningContainer start() throws DataSetException {
        jdbcDatabaseContainer.start();

        DatabaseContainerDataSource dataSource = new DatabaseContainerDataSource(jdbcDatabaseContainer);
        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(dataSource.getConnection());
            DatabaseConfig databaseConfig = databaseConnection.getConfig();
            applyContainerSupportConfig(dbunitConfig, databaseConfig);

            return createRunningContainer(jdbcDatabaseContainer, dataSource, databaseConnection);
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }
    }

    protected void applyContainerSupportConfig(DatabaseConfig containerSupportConfig, DatabaseConfig databaseConfig) {
        databaseConfig.setFeature(FEATURE_BATCHED_STATEMENTS, containerSupportConfig.getFeature(FEATURE_BATCHED_STATEMENTS));
        databaseConfig.setFeature(FEATURE_QUALIFIED_TABLE_NAMES, containerSupportConfig.getFeature(FEATURE_QUALIFIED_TABLE_NAMES));
        databaseConfig.setFeature(FEATURE_CASE_SENSITIVE_TABLE_NAMES, containerSupportConfig.getFeature(FEATURE_CASE_SENSITIVE_TABLE_NAMES));
        databaseConfig.setFeature(FEATURE_DATATYPE_WARNING, containerSupportConfig.getFeature(FEATURE_DATATYPE_WARNING));
        databaseConfig.setFeature(FEATURE_ALLOW_EMPTY_FIELDS, containerSupportConfig.getFeature(FEATURE_ALLOW_EMPTY_FIELDS));

        databaseConfig.setProperty(PROPERTY_STATEMENT_FACTORY, containerSupportConfig.getProperty(PROPERTY_STATEMENT_FACTORY));
        databaseConfig.setProperty(PROPERTY_RESULTSET_TABLE_FACTORY, containerSupportConfig.getProperty(PROPERTY_RESULTSET_TABLE_FACTORY));
        databaseConfig.setProperty(PROPERTY_DATATYPE_FACTORY, containerSupportConfig.getProperty(PROPERTY_DATATYPE_FACTORY));
        databaseConfig.setProperty(PROPERTY_ESCAPE_PATTERN, containerSupportConfig.getProperty(PROPERTY_ESCAPE_PATTERN));
        databaseConfig.setProperty(PROPERTY_TABLE_TYPE, containerSupportConfig.getProperty(PROPERTY_TABLE_TYPE));
        databaseConfig.setProperty(PROPERTY_BATCH_SIZE, containerSupportConfig.getProperty(PROPERTY_BATCH_SIZE));
        databaseConfig.setProperty(PROPERTY_FETCH_SIZE, containerSupportConfig.getProperty(PROPERTY_FETCH_SIZE));
        databaseConfig.setProperty(PROPERTY_METADATA_HANDLER, containerSupportConfig.getProperty(PROPERTY_METADATA_HANDLER));
        databaseConfig.setProperty(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH,
                containerSupportConfig.getProperty(PROPERTY_ALLOW_VERIFYTABLEDEFINITION_EXPECTEDTABLE_COUNT_MISMATCH));
    }

    protected RunningContainer createRunningContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer, DatabaseContainerDataSource dataSource, DatabaseConnection databaseConnection) {
        return new RunningContainer() {
            @Override
            public DataSource getDataSource() {
                return dataSource;
            }

            @Override
            public IDatabaseConnection getDatabaseConnection() {
                return databaseConnection;
            }

            @Override
            public void stop() {
                try {
                    dataSource.close();
                } finally {
                    jdbcDatabaseContainer.stop();
                }
            }
        };
    }
}
