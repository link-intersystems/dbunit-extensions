package com.link_intersystems.dbunit.testcontainers;

import com.link_intersystems.dbunit.database.DatabaseConfigUtils;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DBunitJdbcContainer implements JdbcContainer {

    private static interface RunningContainer extends JdbcContainer {

        void stop();

    }


    private final JdbcDatabaseContainer<?> jdbcDatabaseContainer;
    private DatabaseConfig dbunitConfig;
    private RunningContainer runningContainer;

    public DBunitJdbcContainer(DatabaseContainerSupport databaseContainerSupport) {
        this(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig());
    }

    public DBunitJdbcContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer) {
        this(jdbcDatabaseContainer, new DatabaseConfig());
    }

    public DBunitJdbcContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer, DatabaseConfig dbunitConfig) {
        this.jdbcDatabaseContainer = requireNonNull(jdbcDatabaseContainer);
        this.dbunitConfig = requireNonNull(dbunitConfig);
    }

    @Override
    public JdbcContainerProperties getProperties() {
        return new JdbcDatabaseContainerProperties(jdbcDatabaseContainer);
    }

    public void start() throws DataSetException {
        if (isRunning()) {
            return;
        }

        jdbcDatabaseContainer.start();

        DatabaseContainerDataSource dataSource = new DatabaseContainerDataSource(jdbcDatabaseContainer);
        try {
            DatabaseConnection databaseConnection = new DatabaseConnection(dataSource.getConnection());
            DatabaseConfig connectionConfig = databaseConnection.getConfig();
            DatabaseConfigUtils.copy(dbunitConfig, connectionConfig);

            runningContainer = createRunningContainer(jdbcDatabaseContainer, dataSource, databaseConnection);
        } catch (DatabaseUnitException | SQLException e) {
            throw new DataSetException(e);
        }
    }

    public boolean isRunning() {
        return runningContainer != null;
    }

    public void stop() {
        if (isStopped()) {
            return;
        }

        try {
            runningContainer.stop();
        } finally {
            runningContainer = null;
        }
    }

    public boolean isStopped() {
        return runningContainer == null;
    }

    @Override
    public DataSource getDataSource() {
        if (isStopped()) {
            throw new RuntimeException("Container stopped");
        }

        return runningContainer.getDataSource();
    }

    @Override
    public IDatabaseConnection getDatabaseConnection() {
        if (isStopped()) {
            throw new RuntimeException("Container stopped");
        }
        return runningContainer.getDatabaseConnection();
    }

    protected RunningContainer createRunningContainer(JdbcDatabaseContainer<?> jdbcDatabaseContainer, DatabaseContainerDataSource dataSource, DatabaseConnection databaseConnection) {
        return new RunningContainer() {

            private DatabaseContainerDataSource containerDataSource = dataSource;
            private JdbcDatabaseContainer<?> container = jdbcDatabaseContainer;

            @Override
            public DataSource getDataSource() {
                return containerDataSource;
            }

            @Override
            public IDatabaseConnection getDatabaseConnection() {
                return databaseConnection;
            }

            @Override
            public JdbcContainerProperties getProperties() {
                return DBunitJdbcContainer.this.getProperties();
            }

            @Override
            public void stop() {
                try {
                    containerDataSource.close();
                    containerDataSource = null;
                } finally {
                    container.stop();
                    container = null;
                }
            }
        };
    }
}
