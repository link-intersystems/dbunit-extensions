package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.UnitTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class ColumnPredicatesTest {

    private Column column1;
    private Column column2;

    @BeforeEach
    void setUp() {
        column1 = new Column("col1", DataType.BIGINT);
        column2 = new Column("col2", DataType.VARCHAR);
    }

    @Test
    void byName() {
        Predicate<Column> columnPredicate = ColumnPredicates.byName("col1");

        assertTrue(columnPredicate.test(column1));
        assertFalse(columnPredicate.test(column2));
    }

    @Test
    void byNameIgnoreCase() {
        Predicate<Column> columnPredicate = ColumnPredicates.byNameIgnoreCase("CoL1");

        assertTrue(columnPredicate.test(column1));
        assertFalse(columnPredicate.test(column2));
    }

    @Test
    void byDataType() {
        Predicate<Column> columnPredicate = ColumnPredicates.byDataType(DataType.BIGINT);

        assertTrue(columnPredicate.test(column1));
        assertFalse(columnPredicate.test(column2));
    }
}