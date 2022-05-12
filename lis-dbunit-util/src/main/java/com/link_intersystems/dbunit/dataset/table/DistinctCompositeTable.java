package com.link_intersystems.dbunit.dataset.table;

import org.dbunit.dataset.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DistinctCompositeTable extends AbstractTable {
    private static final Logger logger = LoggerFactory.getLogger(DistinctCompositeTable.class);
    private final ITableMetaData _metaData;
    private final ITable effectiveTable;

    public DistinctCompositeTable(ITable table1, ITable table2) throws DataSetException {
        this(table1.getTableMetaData(), table1, table2);
    }

    public DistinctCompositeTable(String newName, ITable table) throws DataSetException {
        this(new DefaultTableMetaData(newName, table.getTableMetaData().getColumns(), table.getTableMetaData().getPrimaryKeys()), table);
    }

    public DistinctCompositeTable(ITableMetaData metaData, ITable... tables) throws DataSetException {
        this._metaData = metaData;
        this.effectiveTable = mergeTables(metaData, tables);
    }


    private ITable mergeTables(ITableMetaData tableMetaData, ITable... tables) throws DataSetException {
        DefaultTable defaultTable = new DefaultTable(tableMetaData);

        Set<Object> distinctIds = new HashSet<>();

        for (ITable table : tables) {
            for (int i = 0; i < table.getRowCount(); i++) {
                Object pkValue = getPkValue(table, i);

                if (distinctIds.add(pkValue)) {
                    List<Object> values = getValues(table, i, tableMetaData.getColumns());
                    defaultTable.addRow(values.toArray(new Object[0]));
                }
            }
        }

        return defaultTable;
    }

    private Object getPkValue(ITable table, int row) throws DataSetException {
        ITableMetaData tableMetaData = table.getTableMetaData();
        Column[] primaryKeys = tableMetaData.getPrimaryKeys();
        List<Object> pkValues = getValues(table, row, primaryKeys);

        return pkValues;
    }

    private List<Object> getValues(ITable table, int row, Column[] columns) throws DataSetException {
        List<Object> pkValues = new ArrayList<>();

        for (Column primaryKey : columns) {
            Object value = table.getValue(row, primaryKey.getColumnName());
            pkValues.add(value);
        }

        return pkValues;
    }


    public ITableMetaData getTableMetaData() {
        return this._metaData;
    }

    public int getRowCount() {
        return effectiveTable.getRowCount();
    }

    public Object getValue(int row, String columnName) throws DataSetException {
        return effectiveTable.getValue(row, columnName);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(2000);
        sb.append(this.getClass().getName()).append("[");
        sb.append("_metaData=[").append(this._metaData).append("], ");
        sb.append("]");
        return sb.toString();
    }
}
