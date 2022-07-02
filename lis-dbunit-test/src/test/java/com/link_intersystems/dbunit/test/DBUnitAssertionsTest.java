package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import static com.link_intersystems.dbunit.test.DBUnitAssertions.STRICT;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DBUnitAssertionsTest {

    private Column intColumn;
    private Column stringColumn;
    private Column booleanColumn;

    private DefaultTableMetaData defaultTableMetaData1;
    private DefaultTableMetaData defaultTableMetaData2;
    private DefaultTable defaultTable1;
    private DefaultTable defaultTable2;
    private DefaultTableMetaData defaultTableMetaData3;

    @BeforeEach
    void setUp() throws DataSetException {
        intColumn = new Column("a", DataType.BIGINT);
        stringColumn = new Column("b", DataType.VARCHAR);
        booleanColumn = new Column("c", DataType.BOOLEAN);

        defaultTableMetaData1 = new DefaultTableMetaData("A", new Column[]{intColumn, stringColumn});
        defaultTableMetaData2 = new DefaultTableMetaData("B", new Column[]{intColumn, stringColumn});
        defaultTableMetaData3 = new DefaultTableMetaData("B", new Column[]{intColumn, booleanColumn});

        defaultTable1 = new DefaultTable(defaultTableMetaData1);
        defaultTable1.addRow(new Object[]{1, "A"});
        defaultTable1.addRow(new Object[]{2, "B"});

        defaultTable2 = new DefaultTable(defaultTableMetaData1);
        defaultTable2.addRow(new Object[]{1, "A"});
        defaultTable2.addRow(new Object[]{2, "B"});
    }

    @Test
    void assertDataSetEquals() throws DataSetException {
        defaultTable2 = new DefaultTable(defaultTableMetaData2);

        defaultTable2.addRow(new Object[]{1, "A"});
        defaultTable2.addRow(new Object[]{2, "B"});

        DefaultDataSet defaultDataSet1 = new DefaultDataSet();
        defaultDataSet1.addTable(defaultTable1);
        defaultDataSet1.addTable(defaultTable2);

        DefaultDataSet defaultDataSet2 = new DefaultDataSet();
        defaultDataSet2.addTable(defaultTable1);
        defaultDataSet2.addTable(defaultTable2);

        STRICT.assertDataSetEquals(defaultDataSet1, defaultDataSet2);
    }

    @Test
    void assertTablesEquals() throws DataSetException {
        STRICT.assertTableEquals(defaultTable1, defaultTable2);

        defaultTable2.addRow(new Object[]{3, "C"});

        assertThrows(AssertionFailedError.class, () -> STRICT.assertTableEquals(defaultTable1, defaultTable2));
    }

    @Test
    void assertTableContentEquals() throws DataSetException {
        DefaultTable defaultTable1 = new DefaultTable(defaultTableMetaData1);
        defaultTable1.addRow(new Object[]{1, "A"});
        defaultTable1.addRow(new Object[]{2, "B"});

        DefaultTable defaultTable2 = new DefaultTable(defaultTableMetaData1);
        defaultTable2.addRow(new Object[]{1, "A"});
        defaultTable2.addRow(new Object[]{2, "B"});

        STRICT.assertTableContentEquals(defaultTable1, defaultTable2);

        defaultTable2.addRow(new Object[]{3, "C"});

        assertThrows(AssertionFailedError.class, () -> STRICT.assertTableContentEquals(defaultTable1, defaultTable2));
    }

    @Test
    void assertMetaDataEquals() throws DataSetException {
        STRICT.assertMetaDataEquals(defaultTableMetaData1, defaultTableMetaData1);

        assertThrows(AssertionFailedError.class, () -> STRICT.assertMetaDataEquals(defaultTableMetaData1, defaultTableMetaData3));
    }
}