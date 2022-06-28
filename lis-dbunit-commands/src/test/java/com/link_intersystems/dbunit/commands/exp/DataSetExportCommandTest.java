package com.link_intersystems.dbunit.commands.exp;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDatabaseDataSet;
import com.link_intersystems.dbunit.dataset.consumer.DatabaseDataSetConsumer;
import com.link_intersystems.dbunit.table.DatabaseTableReferenceLoader;
import com.link_intersystems.dbunit.table.TableReferenceTraversal;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.test.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaH2DatabaseFactory;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.dialect.SqlDialect;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class DataSetExportCommandTest {

    @Test
    void exportToAnotherDatabase(Connection connection) throws Exception {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);

        DataSetExportCommand dbUnitExportCommand = new DataSetExportCommand(databaseDataSet);
        dbUnitExportCommand.setTables("actor", "film_actor", "film");
        dbUnitExportCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));
        dbUnitExportCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));

        SakilaH2DatabaseFactory sakilaH2DatabaseFactory = new SakilaH2DatabaseFactory("empty");
        H2Database h2Database = sakilaH2DatabaseFactory.create();
        Connection targetConnection = h2Database.getConnection();

        dbUnitExportCommand.setDataSetConsumer(new DatabaseDataSetConsumer(targetConnection));
        dbUnitExportCommand.exec();
    }
}