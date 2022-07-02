package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TestDataSetsTest {

    @Test
    void getTinySakilaDataSet() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();

        assertArrayEquals(new String[]{"actor", "language", "film", "film_actor"}, tinySakilaDataSet.getTableNames());
    }
}