package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDatabaseDataSet;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.jdbc.test.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaH2DatabaseFactory;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.xml.XmlDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class DataSetMigrationCommandTest {

    @Test
    void migrateToAnotherDatabase(Connection connection) throws Exception {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);

        DataSetMigrationCommand migrationCommand = new DataSetMigrationCommand(databaseDataSet);
        migrationCommand.setTables("actor", "film_actor", "film");
        migrationCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));
        migrationCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));

        SakilaH2DatabaseFactory sakilaH2DatabaseFactory = new SakilaH2DatabaseFactory();
        H2Database h2Database = sakilaH2DatabaseFactory.create();
        Connection targetConnection = h2Database.getConnection();
        DatabaseConnection targetDatabaseConnection = new DatabaseConnection(targetConnection);

//        migrationCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.UPDATE);
        migrationCommand.setFlatXmlConsumer("target/flat.xml");
        migrationCommand.exec();
    }

    @Test
    void convertToAnotherFormat() throws Exception {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetMigrationCommand migrateCommand = new DataSetMigrationCommand(tinySakilaDataSet);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        migrateCommand.setXmlConsumer(bout);

        migrateCommand.exec();

        XmlDataSet xmlDataSet = new XmlDataSet(new ByteArrayInputStream(bout.toByteArray()));

        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, xmlDataSet);
    }


}