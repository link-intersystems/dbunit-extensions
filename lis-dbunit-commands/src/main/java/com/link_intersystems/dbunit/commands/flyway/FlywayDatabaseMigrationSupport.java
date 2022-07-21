package com.link_intersystems.dbunit.commands.flyway;

import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.output.MigrateResult;

import javax.sql.DataSource;
import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDatabaseMigrationSupport implements DatabaseMigrationSupport {

    private static final Collection<String> FLYWAY_TABLES = Arrays.asList("flyway_schema_history");
    private MigrationVersion startVersion;
    private MigrationVersion endVersion;
    private String[] locations;
    private boolean filterFlywayTables = true;

    public void setStartVersion(String version) {
        setStartVersion(MigrationVersion.fromVersion(version));
    }

    public void setStartVersion(MigrationVersion startVersion) {
        this.startVersion = startVersion;
    }

    public void setEndVersion(String version) {
        setEndVersion(MigrationVersion.fromVersion(version));
    }

    public void setEndVersion(MigrationVersion endVersion) {
        this.endVersion = endVersion;
    }

    public void setFilterFlywayTables(boolean filterFlywayTables) {
        this.filterFlywayTables = filterFlywayTables;
    }

    public void setLocations(String... locations) {
        this.locations = locations;
    }

    @Override
    public void startDataSet(DataSource dataSource) throws DataSetException {
        migrate(dataSource, startVersion);
    }

    @Override
    public void endDataSet(DataSource dataSource) throws DataSetException {
        migrate(dataSource, endVersion);
    }

    @Override
    public IDataSet decorateResultDataSet(IDatabaseConnection databaseConnection, IDataSet resultDataSet) throws DataSetException {
        if (filterFlywayTables) {
            List<String> tableNames = new ArrayList<>(Arrays.asList(resultDataSet.getTableNames()));
            Iterator<String> resultTableNamesIterator = tableNames.iterator();
            while (resultTableNamesIterator.hasNext()) {
                String resultTableName = resultTableNamesIterator.next();
                if (!acceptResultTable(resultTableName)) {
                    resultTableNamesIterator.remove();
                }
            }
            return new FilteredDataSet(tableNames.toArray(new String[0]), resultDataSet);
        }

        return DatabaseMigrationSupport.super.decorateResultDataSet(databaseConnection, resultDataSet);
    }

    protected boolean acceptResultTable(String resultTableName) {
        return !resultTableName.toLowerCase().startsWith("flyway_");
    }

    protected void migrate(DataSource dataSource, MigrationVersion targetVersion) throws DataSetException {
        FluentConfiguration configuration = Flyway.configure();
        if (locations != null) {
            configuration.locations(locations);
        }
        configuration.dataSource(dataSource);
        configuration.target(targetVersion);

        Flyway flyway = configuration.load();
        migrate(flyway);
    }

    protected void migrate(Flyway flyway) throws DataSetException {
        MigrateResult migrateResult = flyway.migrate();
        if (!migrateResult.success) {
            throw new DataSetException("Unable to setup baseline ");
        }
    }

}
