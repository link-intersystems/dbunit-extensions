package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.filter.IRowFilter;

import java.util.List;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Filters rows based on a column predicate.
 * If a row doesn't contain the specified column it is accepted.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CellRowFilter implements IRowFilter {

    private final String columnName;
    private final Predicate<Object> valueFilter;

    /**
     * Constructs a {@link CellRowFilter} that accepts rows that have a column named
     * by the columnName and contain one of the acceptedValues.
     *
     * @param columnName     the column name to filter.
     * @param acceptedValues the values that are accepted.
     */
    public CellRowFilter(String columnName, List<?> acceptedValues) {
        this(columnName, acceptedValues::contains);
    }

    /**
     * Constructs a {@link CellRowFilter} that accepts rows that have a column named
     * by the columnName and whose values match the valueFilter predicate.
     *
     * @param columnName  the column name to filter.
     * @param valueFilter the value filter predicate.
     */
    public CellRowFilter(String columnName, Predicate<Object> valueFilter) {
        this.columnName = requireNonNull(columnName);
        this.valueFilter = requireNonNull(valueFilter);
    }

    @Override
    public boolean accept(IRowValueProvider rowValueProvider) {
        try {
            return tryAccept(rowValueProvider);
        } catch (NoSuchColumnException e) {
            return handleNoSuchColumnException(e);
        } catch (DataSetException e) {
            return handleDataSetException(e);
        }
    }

    protected boolean tryAccept(IRowValueProvider rowValueProvider) throws DataSetException {
        Object columnValue = rowValueProvider.getColumnValue(columnName);
        return valueFilter.test(columnValue);
    }

    /**
     * Returns true if the column does not exist.
     *
     * @param e the exception that was raised when trying
     *          to access the column value through the {@link IRowValueProvider}.
     * @return always true to accept all rows that don't have a column with
     * the column name of this {@link CellRowFilter}, but can be overridden by subclasses.
     */
    protected boolean handleNoSuchColumnException(NoSuchColumnException e) {
        return true;
    }

    /**
     * Returns false in case of any exception.
     *
     * @param e the exception that was raised when trying
     *          to access the column value through the {@link IRowValueProvider}.
     * @return always false so that no row that raised a {@link DataSetException} is accepted by this filter,
     * but can be overridden by subclasses.
     */
    protected boolean handleDataSetException(DataSetException e) {
        return false;
    }

}