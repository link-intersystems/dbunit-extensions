package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDatabaseDataSet;
import com.link_intersystems.dbunit.table.DatabaseTableOrder;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.jdbc.test.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaSlimTestDBExtension.class)
class DataSetCommandTest {

    private String[] slimDbTableNames = SakilaSlimDB.getTableNames().toArray(new String[0]);

    @Test
    void defaultConsumer(Connection connection) throws Exception {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet sourceDataSet = new DatabaseDataSet(databaseConnection, false);

        DataSetCommand dataSetCommand = new DataSetCommand(sourceDataSet);
        dataSetCommand.setTables(slimDbTableNames);
        dataSetCommand.exec();
    }

    @Test
    void migrateToAnotherDatabase(Connection connection) throws Exception {


        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet sourceDataSet = new DatabaseDataSet(databaseConnection, false);

        DataSetCommand migrationCommand = new DataSetCommand(sourceDataSet);
        migrationCommand.setTables(slimDbTableNames);
        migrationCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));
        Map<Object, Object> replacements = new HashMap<>();
        replacements.put(null, ITable.NO_VALUE);
        migrationCommand.setReplacementObjects(replacements);
        migrationCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));

        DatabaseConnection targetDatabaseConnection = createTargetDatabaseConnection();
        migrationCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.INSERT);

        migrationCommand.exec();

        DatabaseDataSet targetDataSet = new DatabaseDataSet(targetDatabaseConnection, false);

        FilteredDataSet actualTargetDataSet = new FilteredDataSet(slimDbTableNames, targetDataSet);
        FilteredDataSet expectedDataSet = new FilteredDataSet(slimDbTableNames, sourceDataSet);

        DBUnitAssertions.STRICT.assertDataSetEquals(expectedDataSet, actualTargetDataSet);
    }

    private DatabaseConnection createTargetDatabaseConnection() throws SQLException, DatabaseUnitException {
        H2Database targetDatabase = new H2Database();
        Connection targetConnection = targetDatabase.getConnection();
        new SakilaTinyDB().setupDdl(targetConnection);
        DatabaseConnection targetDatabaseConnection = new DatabaseConnection(targetConnection);
        return targetDatabaseConnection;
    }

    @Test
    void convertToAnotherFormat() throws Exception {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetCommand migrateCommand = new DataSetCommand(tinySakilaDataSet);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        migrateCommand.setXmlConsumer(bout);

        migrateCommand.exec();

        XmlDataSet xmlDataSet = new XmlDataSet(new ByteArrayInputStream(bout.toByteArray()));

        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, xmlDataSet);
    }


}