package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TableUtilTest {

    private TableUtil actorUtil;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        ITable actor = tinySakilaDataSet.getTable("actor");
        actorUtil = new TableUtil(actor);
    }

    @Test
    void getRows() throws DataSetException {
        RowList rows = actorUtil.getRows();
        assertEquals(2, rows.size());

        Row row = rows.get(0);
        assertEquals("PENELOPE", row.getValueByColumnName("first_name"));
    }

    @Test
    void testGetRows() throws DataSetException {
        RowList rows = actorUtil.getRows(new String[]{"first_name", "last_name"}, "PENELOPE", "GUINESS");
        assertEquals(1, rows.size());

        Row row = rows.get(0);
        assertEquals("PENELOPE", row.getValueByColumnName("first_name"));
    }

    @Test
    void getPartitionedTables() throws DataSetException {
        ITable[] partitionedTables = actorUtil.getPartitionedTables(1);
        assertEquals(2, partitionedTables.length);

        assertEquals("PENELOPE", partitionedTables[0].getValue(0, "first_name"));
        assertEquals("NICK", partitionedTables[1].getValue(0, "first_name"));
    }
}