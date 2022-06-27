package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.sql.statement.JoinTableReferenceSqlFactory;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
@ExtendWith(SakilaSlimTestDBExtension.class)
class DatabaseTableReferenceLoaderTest {

    private DatabaseConnection databaseConnection;
    private DatabaseTableReferenceLoader tableDependencyLoader;
    private SakilaDBFixture sakilaDBFixture;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(connection);
        sakilaDBFixture = new SakilaDBFixture(connection);
    }

    @Test
    void loadOutgoingTables() throws DataSetException {
        tableDependencyLoader = new DatabaseTableReferenceLoader(databaseConnection, JoinTableReferenceSqlFactory.INSTANCE);

        ITable filmActorTable = sakilaDBFixture.getTable("film_actor");

        TableList tables = tableDependencyLoader.loadOutgoingReferences(filmActorTable);

        assertEquals(2, tables.size());

        assertNotNull(tables.getByName("film"));
        assertNotNull(tables.getByName("actor"));
    }
}