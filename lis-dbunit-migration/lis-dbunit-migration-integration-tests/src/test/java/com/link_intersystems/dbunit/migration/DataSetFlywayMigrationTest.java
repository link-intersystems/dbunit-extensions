package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.consumer.ExternalSortTableConsumer;
import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.table.TableOrder;
import com.link_intersystems.jdbc.test.db.h2.H2Extension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.ext.h2.H2DataTypeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(H2Extension.class)
@SakilaSlimExtension
class DataSetFlywayMigrationTest {

    private DatabaseConnection databaseConnection;

    static Stream<DatabaseDefinition> databases() {
        return Stream.of(
                new DatabaseDefinition("postgres"),
                new DatabaseDefinition("mysql")
        );
    }

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(connection);
        databaseConnection.getConfig().setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new H2DataTypeFactory());
    }

    @ParameterizedTest
    @MethodSource("databases")
    void migrate(DatabaseDefinition databaseDefinition) throws DataSetException, IOException, SQLException {
        DataSetMigration dataSetMigration = new DataSetMigration();

        IDataSet sourceDataSet = new DatabaseDataSet(databaseConnection, true, SakilaTinyDB.getTableNames()::contains);
        sourceDataSet = new FilteredDataSet(new String[]{"language", "film", "actor", "film_actor"}, sourceDataSet);
        dataSetMigration.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        consumerSupport.setCsvConsumer("target/csv");
        IDataSetConsumer csvConsumer = consumerSupport.getDataSetConsumer();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        dataSetMigration.setDataSetConsumers(copyDataSetConsumer, csvConsumer, flatXmlConsumer);

        dataSetMigration.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory(databaseDefinition.getDatabaseContainerSupport()));

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createConfig(databaseDefinition.getScriptsBase());
        dataSetMigration.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

        TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
        dataSetMigration.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(new ExternalSortTableConsumer(tableOrder)));

        dataSetMigration.exec();

        IDataSet migratedDataSet = copyDataSetConsumer.getDataSet();

        ITable migratedActorTable = migratedDataSet.getTable("actor");
        assertEquals(200, migratedActorTable.getRowCount());

        ITable filmDescriptionTable = migratedDataSet.getTable("film_description");
        assertNotNull(filmDescriptionTable);

        ITable film = sourceDataSet.getTable("film");
        DefaultTable filmWithMetaData = new DefaultTable(sourceDataSet.getTableMetaData("film"));
        filmWithMetaData.addTableRows(film);

        for (int rowIndex = 0; rowIndex < filmDescriptionTable.getRowCount(); rowIndex++) {
            assertEquals(filmWithMetaData.getValue(rowIndex, "film_id"), filmDescriptionTable.getValue(rowIndex, "film_id"));
            assertEquals(filmWithMetaData.getValue(rowIndex, "description"), filmDescriptionTable.getValue(rowIndex, "description"));
        }
    }
}