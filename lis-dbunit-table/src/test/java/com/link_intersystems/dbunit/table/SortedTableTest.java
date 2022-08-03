package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class SortedTableTest {

    private ITable actorTable;
    private SortedTable sortedTable;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        actorTable = TestDataSets.getTinySakilaDataSet().getTable("actor");
        Comparator<Row> rowComparator = (o1, o2) -> {
            String value1 = (String) o1.getValue("first_name");
            String value2 = (String) o2.getValue("first_name");
            return value1.compareTo(value2);
        };
        sortedTable = new SortedTable(actorTable, rowComparator);
    }

    @Test
    void getTableMetaData() {
        assertEquals(actorTable.getTableMetaData(), sortedTable.getTableMetaData());
    }

    @Test
    void getRowCount() {
        assertEquals(actorTable.getRowCount(), sortedTable.getRowCount());
    }

    @Test
    void getValue() throws DataSetException {
        assertEquals("PENELOPE", actorTable.getValue(0, "first_name"));
        assertEquals("NICK", actorTable.getValue(1, "first_name"));

        assertEquals("NICK", sortedTable.getValue(0, "first_name"));
        assertEquals("PENELOPE", sortedTable.getValue(1, "first_name"));
    }
}