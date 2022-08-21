package com.link_intersystems.dbunit.table;

import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
@SakilaSlimExtension
class TableListTest {

    private SakilaDBFixture sakilaDBFixture;
    private TableList tableList;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        sakilaDBFixture = new SakilaDBFixture(connection);
        tableList = new TableList(new ArrayList<>());
    }

    @Test
    void addNull() {
        tableList.add(null);

        assertEquals(1, tableList.size());
        assertNull(tableList.get(0));
    }

    @Test
    void pack() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        Collections.addAll(tableList, actors);

        assertEquals(5, tableList.size());
        tableList.pack();
        assertEquals(1, tableList.size());

        ITable actorTable = sakilaDBFixture.getTable("actor");
        assertEquals(actorTable.getRowCount(), tableList.get(0).getRowCount());
    }

    @Test
    void addAtIndex() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableList.add(0, actor);
        }

        assertEquals(5, tableList.size());
    }

    @Test
    void set() throws DataSetException {
        ITable actor = sakilaDBFixture.getTable("actor");
        tableList.add(actor);

        ITable language = sakilaDBFixture.getTable("language");
        tableList.add(language);

        ITable actorTable = sakilaDBFixture.getTable("actor");
        ITable languageTable = tableList.set(1, actorTable);

        assertEquals(sakilaDBFixture.getTable("language").getRowCount(), languageTable.getRowCount());

        assertEquals(2, tableList.size());

        assertEquals(actorTable.getRowCount(), tableList.get(0).getRowCount());
    }

    @Test
    void setNoPrevious() throws DataSetException {
        tableList.add(null);

        ITable previousTableAtIndex = tableList.set(0, sakilaDBFixture.getTable("actor"));

        assertNull(previousTableAtIndex);
        assertEquals(1, tableList.size());
        ITable actorTable = sakilaDBFixture.getTable("actor");
        assertEquals(actorTable.getRowCount(), tableList.get(0).getRowCount());
    }

    @Test
    void removeAtIndex() throws DataSetException {
        ITable actor = sakilaDBFixture.getTable("actor");
        tableList.add(actor);

        ITable language = sakilaDBFixture.getTable("language");
        tableList.add(language);

        tableList.remove(0);


        assertEquals(1, tableList.size());

        assertEquals("language", tableList.get(0).getTableMetaData().getTableName());
    }
}