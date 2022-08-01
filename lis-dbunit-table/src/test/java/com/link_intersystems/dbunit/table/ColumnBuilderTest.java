package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ColumnBuilderTest {

    @Test
    void build() throws DataSetException, IOException {
        ITable actor = TestDataSets.getTinySakilaDataSet().getTable("actor");
        ITableMetaData actorMetaData = actor.getTableMetaData();
        Column firstNameColumn = actorMetaData.getColumns()[actorMetaData.getColumnIndex("first_name")];

        assertEquals(firstNameColumn, new ColumnBuilder(firstNameColumn).build());
    }
}