package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDatabaseDataSet;
import com.link_intersystems.jdbc.test.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaH2DatabaseFactory;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.*;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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

        migrationCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.UPDATE);
        migrationCommand.exec();
    }

    @Test
    void convertToAnotherFormat() throws Exception {
        InputStream in = DataSetMigrationCommandTest.class.getResourceAsStream("flat.xml");
        FlatXmlDataSet flatXmlDataSet = new FlatXmlDataSetBuilder().build(in);
        DataSetMigrationCommand migrateCommand = new DataSetMigrationCommand(flatXmlDataSet);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        migrateCommand.setXmlConsumer(bout);

        migrateCommand.exec();

        XmlDataSet xmlDataSet = new XmlDataSet(new ByteArrayInputStream(bout.toByteArray()));

        assertDataSetsEquals(flatXmlDataSet, xmlDataSet);
    }

    private void assertDataSetsEquals(IDataSet expected, IDataSet actual) throws DataSetException {
        assertArrayEquals(expected.getTableNames(), actual.getTableNames());

        String[] tableNames = expected.getTableNames();

        for (String tableName : tableNames) {
            ITable expectedTable = expected.getTable(tableName);
            ITable actualTable = actual.getTable(tableName);

            assertTableEquals(expectedTable, actualTable);
        }

    }

    private void assertTableEquals(ITable expectedTable, ITable actualTable) throws DataSetException {
        ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        ITableMetaData actualMetaData = actualTable.getTableMetaData();

        assertEquals(expectedMetaData.getTableName(), actualMetaData.getTableName());
        assertArrayEquals(expectedMetaData.getColumns(), actualMetaData.getColumns());
        assertArrayEquals(expectedMetaData.getPrimaryKeys(), actualMetaData.getPrimaryKeys());

        assertEquals(expectedTable.getRowCount(), actualTable.getRowCount());

        Column[] columns = expectedMetaData.getColumns();
        int rowCount = expectedTable.getRowCount();

        for (int row = 0; row < rowCount; row++) {
            for (Column column : columns) {
                String columnName = column.getColumnName();
                assertEquals(expectedTable.getValue(row, columnName), actualTable.getValue(row, columnName), "Value at row" + row + " and column '" + columnName + "'");
            }
        }

    }
}