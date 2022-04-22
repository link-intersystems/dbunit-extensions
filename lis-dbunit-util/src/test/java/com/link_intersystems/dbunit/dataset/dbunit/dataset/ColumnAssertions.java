package com.link_intersystems.dbunit.dataset.dbunit.dataset;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

import static java.text.MessageFormat.format;
import static java.util.Arrays.asList;
import static org.dbunit.dataset.datatype.DataType.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnAssertions {
    public static void assertContains(Column[] columns, String columnName, DataType dataType) {
        boolean contains = contains(columns, columnName, dataType);
        assertTrue(contains, () -> format("Excepted column {0} is not contained in {1}", columnName, asList(columns)));
    }

    public static void assertNotContains(Column[] columns, String columnName) {
        boolean contains = contains(columns, columnName, UNKNOWN);
        assertFalse(contains, () -> format("Excepted column {0} is contained but should not be contained in {1}", columnName, asList(columns)));
    }

    private static boolean contains(Column[] columns, String columnName, DataType dataType) {
        boolean contains = false;
        Column expectedColumn = new Column(columnName, dataType);

        for (int i = 0; i < columns.length && !contains; i++) {
            Column column = columns[i];
            contains = column.equals(expectedColumn);
        }
        return contains;
    }
}
