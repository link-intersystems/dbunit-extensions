package com.link_intersystems.dbunit.flyway;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import org.dbunit.dataset.DataSetException;
import org.flywaydb.core.api.MigrationVersion;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDatabaseMigrationSupport extends AbstractFlywayConfigurationSupport implements DatabaseMigrationSupport {

    private static final Collection<String> FLYWAY_TABLES = Arrays.asList("flyway_schema_history");

    private boolean removeFlywayTables = true;

    private FlywayMigration flywayMigration;
    private Supplier<FlywayMigration> flywayMigrationSupplier = DefaultFlywayMigration::new;

    public void setRemoveFlywayTables(boolean removeFlywayTables) {
        this.removeFlywayTables = removeFlywayTables;
    }

    protected void setFlywayMigrationSupplier(Supplier<FlywayMigration> flywayMigrationSupplier) {
        this.flywayMigrationSupplier = requireNonNull(flywayMigrationSupplier);
    }

    @Override
    public void prepareDataSource(DataSource dataSource) throws DataSetException {
        FlywayMigration flywayMigration = getFlywayMigration();
        flywayMigration.execute(dataSource, getSourceVersion());
    }

    @Override
    public void migrateDataSource(DataSource dataSource) throws DataSetException {
        FlywayMigration flywayMigration = getFlywayMigration();
        flywayMigration.execute(dataSource, getTargetVersion());

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

    private FlywayMigration getFlywayMigration() {
        if (flywayMigration == null) {
            flywayMigration = flywayMigrationSupplier.get();
            flywayMigration.apply(this);
        }
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
