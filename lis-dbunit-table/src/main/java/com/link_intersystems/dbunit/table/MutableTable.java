package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableMetaDataUtil;
import com.link_intersystems.util.Serialization;
import org.dbunit.dataset.*;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.io.Serializable;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MutableTable extends AbstractTable {


    private final TableUtil thisTableUtil;

    static class RowOverride {
        private Map<String, Object> cellValues = new HashMap<>();

        RowOverride() {
        }

        RowOverride(RowOverride rowOverride) {
            Set<Map.Entry<String, Object>> rowOverrideEntries = rowOverride.cellValues.entrySet();
            for (Map.Entry<String, Object> rowOverrideEntry : rowOverrideEntries) {
                cellValues.put(rowOverrideEntry.getKey(), tryCopy(rowOverrideEntry.getValue()));
            }
        }

        private Object tryCopy(Object value) {
            if (value == null) {
                return null;
            }

            Object copy = null;

            if (value instanceof Cloneable) {
                try {
                    Method clone = Object.class.getDeclaredMethod("clone");
                    clone.setAccessible(true);
                    copy = clone.invoke(value);
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException |
                         InaccessibleObjectException e) {
                }
            }

            if (copy == null && value instanceof Serializable) {
                copy = Serialization.clone((Serializable) value);
            }

            if (copy == null) {
                copy = value;
            }

            return copy;
        }

        public void setValue(Column column, Object columnValue) throws TypeCastException {
            String columnName = column.getColumnName();
            DataType dataType = column.getDataType();
            Object castedValue = dataType.typeCast(columnValue);
            cellValues.put(columnName, castedValue);
        }

        public Object getValue(String columnName) {
            return cellValues.get(columnName);
        }

        public boolean hasValue(String columnName) {
            return cellValues.containsKey(columnName);
        }
    }

    private final TableMetaDataUtil baseMetaDataUtil;
    private ITable baseTable;

    private SortedMap<Integer, RowOverride> rowOverrides = new TreeMap<>();

    public MutableTable(ITable baseTable) {
        this.baseTable = requireNonNull(baseTable);
        thisTableUtil = new TableUtil(this);
        baseMetaDataUtil = new TableMetaDataUtil(baseTable.getTableMetaData());
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return baseTable.getTableMetaData();
    }

    @Override
    public int getRowCount() {
        if (!rowOverrides.isEmpty()) {
            return Math.max(rowOverrides.lastKey() + 1, baseTable.getRowCount());
        }

        return baseTable.getRowCount();
    }

    @Override
    public Object getValue(int rowIndex, String columnName) throws DataSetException {
        if (rowIndex >= getRowCount()) {
            throw new DataSetException(new IndexOutOfBoundsException("row index"));
        }

        Object value = null;

        RowOverride rowOverride = rowOverrides.get(rowIndex);
        if (rowOverride != null && rowOverride.hasValue(columnName)) {
            value = rowOverride.getValue(columnName);
        } else if (rowIndex < baseTable.getRowCount()) {
            value = baseTable.getValue(rowIndex, columnName);
        }

        return value;
    }

    public void setValue(int rowIndex, String columnName, Object columnValue) throws DataSetException {
        Column column = baseMetaDataUtil.getColumn(columnName);

        RowOverride rowOverride = rowOverrides.computeIfAbsent(rowIndex, i -> new RowOverride());
        rowOverride.setValue(column, columnValue);
    }

    public void setValues(int rowIndex, Object... values) throws DataSetException {
        ITableMetaData tableMetaData = baseTable.getTableMetaData();
        Column[] columns = tableMetaData.getColumns();

        for (int columnIndex = 0; columnIndex < columns.length; columnIndex++) {
            String columnName = columns[columnIndex].getColumnName();

            Object cellValue;

            if (columnIndex < values.length) {
                cellValue = values[columnIndex];
            } else {
                cellValue = getValue(rowIndex, columnName);
            }

            setValue(rowIndex, columnName, cellValue);
        }
    }

    public Row getValues(int rowIndex) throws DataSetException {
        return thisTableUtil.getRow(rowIndex);
    }

    public void applyMutationsFrom(MutableTable otherMutableTable) {
        SortedMap<Integer, RowOverride> otherRowOverrides = otherMutableTable.rowOverrides;
        otherRowOverrides.entrySet().stream().forEach(e -> rowOverrides.put(e.getKey(), new RowOverride(e.getValue())));
    }

}
