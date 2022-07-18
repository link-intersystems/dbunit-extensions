package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TableMetaDataUtilTest {

    private TableMetaDataUtil tableMetaDataUtil;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        ITable actor = TestDataSets.getTinySakilaDataSet().getTable("actor");
        ITableMetaData tableMetaData = actor.getTableMetaData();

        DefaultTableMetaData metaWithPK = new DefaultTableMetaData(tableMetaData.getTableName(), tableMetaData.getColumns(), new String[]{"actor_id"});
        tableMetaDataUtil = new TableMetaDataUtil(metaWithPK);
    }

    @Test
    void getColumn() throws DataSetException {
        Column firstName = tableMetaDataUtil.getColumn("first_name");

        assertNotNull(firstName);
    }

    @Test
    void getPrimaryKeys() throws DataSetException {
        Column[] pkColumns = tableMetaDataUtil.getPrimaryKeys();

        assertArrayEquals(new Column[]{tableMetaDataUtil.getColumn("actor_id")}, pkColumns);
    }

    @Test
    void isPrimaryKey() throws DataSetException {
        assertFalse(tableMetaDataUtil.isPrimaryKey("first_name"));
        assertTrue(tableMetaDataUtil.isPrimaryKey("actor_id"));
    }
}