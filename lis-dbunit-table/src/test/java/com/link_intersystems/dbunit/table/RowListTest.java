package com.link_intersystems.dbunit.table;


import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
@ExtendWith(SakilaSlimTestDBExtension.class)
class RowListTest {

    private SakilaDBFixture sakilaDBFixture;
    private ITable actorTable;
    private RowList actorRows;
    private TableUtil actorTableUtil;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        sakilaDBFixture = new SakilaDBFixture(connection);

        actorTable = sakilaDBFixture.getTable("actor");
        actorTableUtil = new TableUtil(actorTable);
        actorRows = new RowList(actorTable.getTableMetaData());
    }

    @Test
    void add() throws DataSetException {
        List<Row> rows = actorTableUtil.getRows();
        actorRows.addAll(rows);

        assertEquals(200, actorRows.size());
    }

    @Test
    void set() throws DataSetException {
        Row row0 = actorTableUtil.getRow(0);
        actorRows.add(row0);

        Row row1 = actorTableUtil.getRow(1);
        Row previousRow = actorRows.set(0, row1);

        assertEquals(row0, previousRow);
        assertEquals(1, actorRows.size());
        assertEquals(row1, actorRows.get(0));
    }

    @Test
    void remove() throws DataSetException {
        Row row0 = actorTableUtil.getRow(0);
        actorRows.add(row0);

        Row row1 = actorTableUtil.getRow(1);
        actorRows.add(row1);

        actorRows.remove(1);

        assertEquals(1, actorRows.size());
        assertEquals(row0, actorRows.get(0));
    }

    @Test
    void indexOf() throws DataSetException {
        ITable actorTable = sakilaDBFixture.getTable("actor");
        TableUtil actorUtil = new TableUtil(actorTable);
        Row row1 = actorUtil.getRow(1);
        RowList rows = actorUtil.getRows();

        assertEquals(1, rows.indexOf(row1));
    }

    @Test
    void indexOfLastElement() throws DataSetException {
        ITable actorTable = sakilaDBFixture.getTable("actor");
        TableUtil actorUtil = new TableUtil(actorTable);
        int lastElementIndex = actorTable.getRowCount() - 1;
        Row lastRow = actorUtil.getRow(lastElementIndex);
        RowList rows = actorUtil.getRows();

        assertEquals(lastElementIndex, rows.indexOf(lastRow));
    }

    @Test
    void indexOfFirstElement() throws DataSetException {
        ITable actorTable = sakilaDBFixture.getTable("actor");
        TableUtil actorUtil = new TableUtil(actorTable);
        Row firstRow = actorUtil.getRow(0);
        RowList rows = actorUtil.getRows();

        assertEquals(0, rows.indexOf(firstRow));
    }


    @Test
    void indexOfUnknownElement() throws DataSetException {
        ITable actorTable = sakilaDBFixture.getTable("actor");
        TableUtil actorUtil = new TableUtil(actorTable);
        RowList rows = actorUtil.getRows();

        assertEquals(-1, rows.indexOf("row1"));
    }

    @Test
    void addIncompatibleRow() throws DataSetException {
        ITable languageTable = sakilaDBFixture.getTable("language");
        TableUtil languageTableUtil = new TableUtil(languageTable);
        Row row = languageTableUtil.getRow(0);

        assertThrows(IllegalArgumentException.class, () -> actorRows.add(row));
    }

}