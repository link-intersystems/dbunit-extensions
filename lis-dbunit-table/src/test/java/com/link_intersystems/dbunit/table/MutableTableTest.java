package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class MutableTableTest {

    private IDataSet tinySakilaDataSet;
    private ITable actor;
    private MutableTable mutableActor;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        actor = tinySakilaDataSet.getTable("actor");

        mutableActor = new MutableTable(actor);
    }

    @Test
    void setCellValue() throws DataSetException {
        Object oldName = mutableActor.getValue(1, "first_name");
        assertEquals(oldName, "NICK");

        mutableActor.setValue(1, "first_name", "TEST");

        Object newName = mutableActor.getValue(1, "first_name");

        assertNotEquals(oldName, newName);
        assertEquals(newName, "TEST");
    }

    @Test
    void setRow() throws DataSetException {
        Object oldName = mutableActor.getValue(1, "first_name");
        assertEquals(oldName, "NICK");

        mutableActor.setValues(1, new Object[]{"20", "TEST", "TEST_LAST"});

        Row row = mutableActor.getValues(1);
        assertEquals(Arrays.asList("20", "TEST", "TEST_LAST", "2006-02-15 04:34:33.0"), row);

        assertEquals(actor.getRowCount(), mutableActor.getRowCount());
    }

    @Test
    void addRow() throws DataSetException {
        assertEquals(2, mutableActor.getRowCount());

        Object oldName = mutableActor.getValue(1, "first_name");
        assertEquals(oldName, "NICK");


        mutableActor.setValues(3, new Object[]{"20", "TEST", "TEST_LAST", "2006-02-15 04:34:33.0"});

        Row row1 = mutableActor.getValues(1);
        assertEquals(Arrays.asList("2", "NICK", "WAHLBERG", "2006-02-15 04:34:33.0"), row1);

        Row row2 = mutableActor.getValues(2);
        assertEquals(Arrays.asList(null, null, null, null), row2);

        Row row3 = mutableActor.getValues(3);
        assertEquals(Arrays.asList("20", "TEST", "TEST_LAST", "2006-02-15 04:34:33.0"), row3);

        assertEquals(4, mutableActor.getRowCount());
    }

    @Test
    void getRowCount() {
        assertEquals(actor.getRowCount(), mutableActor.getRowCount());
    }
}