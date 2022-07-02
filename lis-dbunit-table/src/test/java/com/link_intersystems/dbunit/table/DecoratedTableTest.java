package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.link_intersystems.dbunit.test.DBUnitAssertions.STRICT;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DecoratedTableTest {

    private ITable actor;
    private DecoratedTable decoratedTable;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        actor = tinySakilaDataSet.getTable("actor");

        decoratedTable = new DecoratedTable(actor, "actor_id");
    }

    @Test
    void getTableMetaData() throws DataSetException {
        ITableMetaData decoratedMetaData = decoratedTable.getTableMetaData();

        assertNotEquals(actor.getTableMetaData(), decoratedMetaData);

        Column[] primaryKeys = decoratedMetaData.getPrimaryKeys();
        assertArrayEquals(new Column[]{new Column("actor_id", DataType.UNKNOWN)}, primaryKeys);
    }

    @Test
    void tableContentEquals() throws DataSetException {
        STRICT.assertTableContentEquals(actor, decoratedTable);
    }
}