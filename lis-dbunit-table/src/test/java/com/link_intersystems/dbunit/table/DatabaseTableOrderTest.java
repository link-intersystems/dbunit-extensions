package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaSlimExtension
class DatabaseTableOrderTest {

    private DatabaseTableOrder databaseTableOrder;

    @BeforeEach
    void setUp(Connection sakilaConnection) throws DatabaseUnitException {
        DatabaseConnection databaseConnection = new DatabaseConnection(sakilaConnection);
        databaseTableOrder = new DatabaseTableOrder(databaseConnection);
    }

    @Test
    void orderTables() throws DataSetException {
        String[] orderdTables = databaseTableOrder.orderTables("film_actor", "film", "language", "actor");

        assertArrayEquals(new String[]{"language", "film", "actor", "film_actor"}, orderdTables);
    }

    @Test
    void reverseOrder() throws DataSetException {
        String[] orderdTables = databaseTableOrder.reverse().orderTables("film_actor", "film", "language", "actor");

        assertArrayEquals(new String[]{"film_actor", "actor", "film", "language"}, orderdTables);
    }
}