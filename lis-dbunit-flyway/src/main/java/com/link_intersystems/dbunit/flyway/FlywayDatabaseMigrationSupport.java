package com.link_intersystems.dbunit.flyway;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDatabaseMigrationSupport implements DatabaseMigrationSupport {

    private static final Collection<String> FLYWAY_TABLES = Arrays.asList("flyway_schema_history");

    private boolean removeFlywayTables = true;

    private FlywayMigration flywayMigration = new DefaultFlywayMigration();
    private FlywayDataSetMigrationConfig migrationConfig = new FlywayDataSetMigrationConfig();

    public void setRemoveFlywayTables(boolean removeFlywayTables) {
        this.removeFlywayTables = removeFlywayTables;
    }

    public void setFlywayMigration(FlywayMigration flywayMigration) {
        this.flywayMigration = Objects.requireNonNull(flywayMigration);
    }

    public void setMigrationConfig(FlywayDataSetMigrationConfig migrationConfig) {
        this.migrationConfig = migrationConfig;
    }

    public FlywayDataSetMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    @Override
    public void prepareDataSource(DataSource dataSource) throws DataSetException {
        FlywayMigration flywayMigration = getFlywayMigration();
        FlywayDataSetMigrationConfig migrationConfig = getMigrationConfig();

        flywayMigration.execute(dataSource, migrationConfig.getSourceVersion());
    }

    @Override
    public void migrateDataSource(DataSource dataSource) throws DataSetException {
        FlywayMigration flywayMigration = getFlywayMigration();
        FlywayDataSetMigrationConfig migrationConfig = getMigrationConfig();

        flywayMigration.execute(dataSource, migrationConfig.getTargetVersion());

        afterMigrate(dataSource);
    }

    protected void afterMigrate(DataSource dataSource) throws DataSetException {
        if (removeFlywayTables) {
            try (Connection connection = dataSource.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    for (String flywayTable : getFlywayTables()) {
                        dropTable(statement, flywayTable);
                    }
                }
            } catch (SQLException e) {
                throw new DataSetException(e);
            }
        }
    }

    public FlywayMigration getFlywayMigration() {
        return flywayMigration;
    }

    protected Collection<String> getFlywayTables() {
        return FLYWAY_TABLES;
    }

    protected void dropTable(Statement statement, String flywayTable) {
        try {
            statement.execute("drop table " + flywayTable);
        } catch (SQLException e) {
            // TODO log
        }
    }


}
