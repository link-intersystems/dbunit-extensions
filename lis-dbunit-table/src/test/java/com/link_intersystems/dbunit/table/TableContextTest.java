package com.link_intersystems.dbunit.table;

import com.link_intersystems.test.db.sakila.SakilaSlimTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
@ExtendWith(SakilaSlimTestDBExtension.class)
class TableContextTest {

    private SakilaDBFixture sakilaDBFixture;
    private TableContext tableContext;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        sakilaDBFixture = new SakilaDBFixture(connection);
        tableContext = new TableContext();
    }

    @Test
    void addNull() {
        tableContext.add(null);

        assertEquals(1, tableContext.size());
        assertNull(tableContext.get(0));
    }

    @Test
    void add() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableContext.add(actor);
        }

        assertEquals(1, tableContext.size());

        ITable actorTable = sakilaDBFixture.getTable("actor");
        assertEquals(actorTable.getRowCount(), tableContext.get(0).getRowCount());
    }

    @Test
    void addAtIndex() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableContext.add(0, actor);
        }

        assertEquals(1, tableContext.size());

        ITable actorTable = sakilaDBFixture.getTable("actor");
        assertEquals(actorTable.getRowCount(), tableContext.get(0).getRowCount());
    }

    @Test
    void set() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableContext.add(actor);
        }
        ITable[] languages = sakilaDBFixture.getSplittedTables("language", 2);
        for (ITable language : languages) {
            tableContext.add(language);
        }

        ITable actorTable = sakilaDBFixture.getTable("actor");
        ITable languageTable = tableContext.set(1, actorTable);
        assertEquals(sakilaDBFixture.getTable("language").getRowCount(), languageTable.getRowCount());

        assertEquals(2, tableContext.size());

        assertNull(tableContext.get(0));
        assertEquals(actorTable.getRowCount(), tableContext.get(1).getRowCount());
    }

    @Test
    void setNoPrevious() throws DataSetException {
        tableContext.add(null);

        ITable previousTableAtIndex = tableContext.set(0, sakilaDBFixture.getTable("actor"));

        assertNull(previousTableAtIndex);
        assertEquals(1, tableContext.size());
        ITable actorTable = sakilaDBFixture.getTable("actor");
        assertEquals(actorTable.getRowCount(), tableContext.get(0).getRowCount());
    }

    @Test
    void removeAtIndex() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableContext.add(actor);
        }
        ITable[] languages = sakilaDBFixture.getSplittedTables("language", 2);
        for (ITable language : languages) {
            tableContext.add(language);
        }

        tableContext.remove(0);


        assertEquals(1, tableContext.size());

        ITable language = sakilaDBFixture.getTable("language");
        assertEquals(language.getRowCount(), tableContext.get(0).getRowCount());
    }

    @Test
    void getSnapshot() throws DataSetException {
        ITable[] actors = sakilaDBFixture.getSplittedTables("actor", 40);
        for (ITable actor : actors) {
            tableContext.add(actor);
        }

        ListSnapshot<ITable> snapshot1 = tableContext.getSnapshot();

        ITable[] languages = sakilaDBFixture.getSplittedTables("language", 2);
        for (ITable language : languages) {
            tableContext.add(language);
        }

        ListSnapshot<ITable> snapshot2 = tableContext.getSnapshot();

        List<ITable> diff = snapshot1.diff(snapshot2);
        assertEquals(1, diff.size());

        ITable language = sakilaDBFixture.getTable("language");
        assertEquals(language.getRowCount(), diff.get(0).getRowCount());
    }
}