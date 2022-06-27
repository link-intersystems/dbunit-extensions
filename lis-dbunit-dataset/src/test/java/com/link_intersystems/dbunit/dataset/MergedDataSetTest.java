package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class MergedDataSetTest {

    public static final String COL_ID = "id";
    public static final String COL_FIRST_NAME = "first_name";
    public static final String COL_LAST_NAME = "last_name";
    private static final String COL_NAME = "name";

    private MergedDataSet mergedDataSet;

    @BeforeEach
    void setUp() throws DataSetException {
        Column[] actorColumns = new Column[]{
                new Column(COL_ID, DataType.BIGINT),
                new Column(COL_FIRST_NAME, DataType.VARCHAR),
                new Column(COL_LAST_NAME, DataType.VARCHAR)
        };

        DefaultTableMetaData actorMetadata = new DefaultTableMetaData("actor", actorColumns, new String[]{"id"});

        DefaultTable actor1 = new DefaultTable(actorMetadata);
        actor1.addRow(new Object[]{1, "PENELOPE", "GUINESS"});
        actor1.addRow(new Object[]{2, "NICK", "WAHLBERG"});

        DefaultTable actor2 = new DefaultTable(actorMetadata);
        actor2.addRow(new Object[]{1, "PENELOPE", "GUINESS"});


        DefaultTable actor3 = new DefaultTable(actorMetadata);
        actor3.addRow(new Object[]{2, "NICK", "WAHLBERG"});
        actor3.addRow(new Object[]{3, "ED", "CHASE"});


        Column[] filmColumns = new Column[]{
                new Column(COL_ID, DataType.BIGINT),
                new Column(COL_NAME, DataType.VARCHAR)
        };

        DefaultTableMetaData filmMetadata = new DefaultTableMetaData("film", filmColumns, new String[]{"id"});

        DefaultTable film = new DefaultTable(filmMetadata);
        film.addRow(new Object[]{1, "ACADEMY DINOSAUR"});
        film.addRow(new Object[]{2, "ACE GOLDFINGER"});

        mergedDataSet = new MergedDataSet(actor1, film, actor2, actor3);
    }

    @Test
    void createIterator() throws DataSetException {
        ITableIterator iterator = mergedDataSet.createIterator(false);

        assertTrue(iterator.next());

        ITable table = iterator.getTable();

        assertEquals(3, table.getRowCount());

        assertEquals(1, table.getValue(0, COL_ID));
        assertEquals("PENELOPE", table.getValue(0, COL_FIRST_NAME));
        assertEquals("GUINESS", table.getValue(0, COL_LAST_NAME));

        assertEquals(2, table.getValue(1, COL_ID));
        assertEquals("NICK", table.getValue(1, COL_FIRST_NAME));
        assertEquals("WAHLBERG", table.getValue(1, COL_LAST_NAME));

        assertEquals(3, table.getValue(2, COL_ID));
        assertEquals("ED", table.getValue(2, COL_FIRST_NAME));
        assertEquals("CHASE", table.getValue(2, COL_LAST_NAME));

        assertTrue(iterator.next());
        assertFalse(iterator.next());
    }

    @Test
    void reversedIterator() throws DataSetException {
        ITableIterator iterator = mergedDataSet.createIterator(true);

        assertTrue(iterator.next());
        assertEquals("film", iterator.getTable().getTableMetaData().getTableName());
        assertTrue(iterator.next());
        assertEquals("actor", iterator.getTable().getTableMetaData().getTableName());
        assertFalse(iterator.next());
    }
}