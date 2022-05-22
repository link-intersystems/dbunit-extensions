package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.sql.statement.JoinDependencyStatementFactory;
import com.link_intersystems.test.db.sakila.SakilaTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
@ExtendWith(SakilaTestDBExtension.class)
class TableDependencyLoaderWithJoinTest {

    private DatabaseConnection databaseConnection;
    private TableDependencyLoader tableDependencyLoader;
    private SakilaDBFixture sakilaDBFixture;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(connection);
        sakilaDBFixture = new SakilaDBFixture(connection);
    }

    @Test
    void loadOutgoingTables() throws DataSetException {
        tableDependencyLoader = new TableDependencyLoader(databaseConnection, JoinDependencyStatementFactory.INSTANCE);

        ITable filmActorTable = sakilaDBFixture.getTable("film_actor");
        TableContext tableContext = new TableContext();

        tableDependencyLoader.loadOutgoingTables(filmActorTable, tableContext);

        assertEquals(2, tableContext.size());

        Map<String, ITable> tableContextMap = tableContext.toMap();
        assertTrue(tableContextMap.containsKey("film"));
        assertTrue(tableContextMap.containsKey("actor"));
    }
}