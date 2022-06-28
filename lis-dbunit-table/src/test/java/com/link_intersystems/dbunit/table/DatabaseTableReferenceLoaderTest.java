package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.sql.statement.JoinTableReferenceSqlFactory;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceList;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

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
    private ConnectionMetaData connectionMetaData;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(connection);
        connectionMetaData = new ConnectionMetaData(connection);
        sakilaDBFixture = new SakilaDBFixture(connection);
    }

    @Test
    void loadOutgoingTables() throws DataSetException, SQLException {
        tableDependencyLoader = new DatabaseTableReferenceLoader(databaseConnection, JoinTableReferenceSqlFactory.INSTANCE);

        ITable filmActorTable = sakilaDBFixture.getTable("film_actor");

        TableReferenceList outgoingReferences = connectionMetaData.getOutgoingReferences("film_actor");
        TableList tables = tableDependencyLoader.loadReferencedTables(filmActorTable, outgoingReferences, TableReference.Direction.NATURAL);

        assertEquals(2, tables.size());

        assertNotNull(tables.getByName("film"));
        assertNotNull(tables.getByName("actor"));
    }
}