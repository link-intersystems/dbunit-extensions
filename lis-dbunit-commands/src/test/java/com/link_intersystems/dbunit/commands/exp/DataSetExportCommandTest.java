package com.link_intersystems.dbunit.commands.exp;

import com.link_intersystems.dbunit.table.DatabaseTableReferenceLoader;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.DatabaseSequenceFilter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class DataSetExportCommandTest {

    @Test
    void export(Connection connection) throws Exception {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);

        DataSetExportCommand dbUnitExportCommand = new DataSetExportCommand(databaseDataSet);
        dbUnitExportCommand.setConsistentResult(new DatabaseTableReferenceLoader(databaseConnection));
        dbUnitExportCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));
        dbUnitExportCommand.setTables("actor", "film_actor", "film");
        dbUnitExportCommand.exec();
    }
}