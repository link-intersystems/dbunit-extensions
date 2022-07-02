package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static com.link_intersystems.dbunit.test.DBUnitAssertions.STRICT;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class MergedTableTest {

    private ITable actor;
    private ITable actorWithPks;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();

        actor = tinySakilaDataSet.getTable("actor");
        actorWithPks = new DecoratedTable(actor, "actor_id");

        STRICT.assertTableContentEquals(actor, actorWithPks);
    }

    @Test
    void mergeTables() throws DataSetException {
        MergedTable mergedTable = new MergedTable(actorWithPks, actorWithPks);

        STRICT.assertTableEquals(actorWithPks, mergedTable);
    }

    @Test
    void mergeTablesWithoutPKs() {
        assertThrows(DataSetException.class, () -> new MergedTable(actor, actor));
    }

    @Test
    void toStringTest() throws DataSetException {
        String string = new MergedTable(actorWithPks, actorWithPks).toString();
        assertNotNull(string);
    }
}