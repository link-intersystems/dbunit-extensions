package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaSlimExtension
class DataSetBuilderTest {

    @Test
    void filterTables() throws Exception {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetBuilder dataSetBuilder = new DataSetBuilder();
        dataSetBuilder.setSourceDataSetSupplier(() -> tinySakilaDataSet);
        dataSetBuilder.setTables("actor");
        IDataSet build = dataSetBuilder.build();

        assertEquals(1, build.getTableNames().length);
        ITable actor = build.getTable("actor");

        ITable expectedTable = tinySakilaDataSet.getTable("actor");

        DBUnitAssertions.STRICT.assertTableEquals(expectedTable, actor);
    }

    @Test
    void filterRows() throws Exception {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetBuilder dataSetBuilder = new DataSetBuilder();
        dataSetBuilder.setSourceDataSetSupplier(() -> tinySakilaDataSet);
        dataSetBuilder.setTableContentFilter(t -> t.getTableName().equals("language") ? rvp -> false : null);

        IDataSet dataSet = dataSetBuilder.build();

        ITable inputTable = tinySakilaDataSet.getTable("language");
        ITable outputTable = dataSet.getTable("language");

        assertEquals(1, inputTable.getRowCount());
        assertEquals(0, outputTable.getRowCount());

    }
}