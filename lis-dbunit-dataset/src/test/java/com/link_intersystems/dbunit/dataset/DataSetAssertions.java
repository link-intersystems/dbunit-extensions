package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetAssertions {

    private IDataSet dataSet;

    public DataSetAssertions(IDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public void assertRowCount(String tableName, int expectedRowCount) throws DataSetException {
        ITable table = dataSet.getTable(tableName);
        assertEquals(expectedRowCount, table.getRowCount(), tableName + " entity count");
    }
}
