package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.UnitTest;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.NoSuchColumnException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class CellRowFilterTest {

    private Predicate<Object> predicate;
    private IRowValueProvider rowValueProvider;
    private CellRowFilter filter;

    @BeforeEach
    void setUp() {
        predicate = mock(Predicate.class);
        rowValueProvider = mock(IRowValueProvider.class);
        filter = new CellRowFilter("name", predicate);
    }

    @Test
    void accept() throws DataSetException {
        when(rowValueProvider.getColumnValue("name")).thenReturn("Test");
        when(predicate.test("Test")).thenReturn(true);

        assertTrue(filter.accept(rowValueProvider));
    }

    @Test
    void notAcceptByPredicate() throws DataSetException {
        when(rowValueProvider.getColumnValue("name")).thenReturn("Test");
        when(predicate.test("Test")).thenReturn(false);

        assertFalse(filter.accept(rowValueProvider));
    }

    @Test
    void notAcceptByDataSetException() throws DataSetException {
        when(rowValueProvider.getColumnValue("name")).thenThrow(new DataSetException());

        assertFalse(filter.accept(rowValueProvider));
    }

    @Test
    void acceptedWhenColumnDoesNotExist() throws DataSetException {
        when(rowValueProvider.getColumnValue("name")).thenThrow(new NoSuchColumnException("someTable", ""));

        assertTrue(filter.accept(rowValueProvider));
    }


    @Test
    void usingFilterValues() throws DataSetException {
        filter = new CellRowFilter("name", asList("A", "B"));
        when(rowValueProvider.getColumnValue("name")).thenReturn("A").thenReturn("C").thenReturn("B");


        assertTrue(filter.accept(rowValueProvider));
        assertFalse(filter.accept(rowValueProvider));
        assertTrue(filter.accept(rowValueProvider));
    }
}