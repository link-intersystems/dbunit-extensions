package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class TableReferenceTraversalTest {

    private TableReferenceTraversal tableReferenceTraversal;
    private DatabaseDataSet databaseDataSet;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException, SQLException {

        ConnectionMetaData connectionMetaData = new ConnectionMetaData(connection);
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        databaseDataSet = new DatabaseDataSet(databaseConnection, false);
        tableReferenceTraversal = new TableReferenceTraversal(connectionMetaData, new DatabaseTableReferenceLoader(databaseConnection));
    }

    @Test
    void traverseOutgoingReferences() throws DataSetException {
        ITable filmActor = databaseDataSet.getTable("film_actor");
        TableUtil filmActorUtil = new TableUtil(filmActor);
        Row rowById = filmActorUtil.getRowById(1, 1);
        DefaultTable defaultTable = new DefaultTable(filmActor.getTableMetaData());
        defaultTable.addRow(rowById.toArray());


        TableList tableList = tableReferenceTraversal.traverseOutgoingReferences(defaultTable);

        assertEquals(2, tableList.size());
        ITable filmTable = tableList.getByName("film");
        assertNotNull(filmTable);
        assertEquals(1, filmTable.getRowCount());
        Object film_id = filmTable.getValue(0, "film_id");
        assertEquals(1, film_id);

        ITable actorTable = tableList.getByName("actor");
        assertNotNull(actorTable);
        assertEquals(1, actorTable.getRowCount());
        Object actor_id = actorTable.getValue(0, "actor_id");
        assertEquals(1, actor_id);
    }
}