package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.*;

import java.util.*;

import static java.text.MessageFormat.format;

/**
 * Merged two or more tables by the primary key definition so that the result table's rows are distinct.
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MergedTable extends AbstractTable {
    private final ITableMetaData _metaData;
    private final ITable effectiveTable;

    public MergedTable(ITable table1, ITable table2) throws DataSetException {
        this(table1.getTableMetaData(), table1, table2);
    }

    public MergedTable(String newName, ITable table) throws DataSetException {
        this(new DefaultTableMetaData(newName, table.getTableMetaData().getColumns(), table.getTableMetaData().getPrimaryKeys()), table);
    }

    public MergedTable(ITable table, ITable... moreTables) throws DataSetException {
        this(table.getTableMetaData(), add(moreTables, table));
    }

    private static ITable[] add(ITable[] sourceTables, ITable toAdd) {
        ITable[] result = new ITable[sourceTables.length + 1];
        System.arraycopy(sourceTables, 0, result, 0, sourceTables.length);
        result[sourceTables.length] = toAdd;
        return result;
    }

    public MergedTable(ITableMetaData metaData, ITable... tables) throws DataSetException {
        this._metaData = metaData;
        for (ITable table : tables) {
            if (!tableMetaDataEquals(metaData, table.getTableMetaData())) {
                String msg = format("tables are not compatible with the metadata {0}", metaData);
                throw new IllegalArgumentException(msg);
            }
        }
        this.effectiveTable = mergeTables(metaData, tables);
    }

    private boolean tableMetaDataEquals(ITableMetaData metaData1, ITableMetaData metaData2) throws DataSetException {
        if (!metaData1.getTableName().equals(metaData2.getTableName())) {
            return false;
        }

        if (!Arrays.equals(metaData1.getColumns(), metaData2.getColumns())) {
            return false;
        }

        return Arrays.equals(metaData1.getPrimaryKeys(), metaData2.getPrimaryKeys());
    }


    private ITable mergeTables(ITableMetaData tableMetaData, ITable... tables) throws DataSetException {
        DefaultTable defaultTable = new DefaultTable(tableMetaData);

        Set<Object> distinctIds = new HashSet<>();

        for (ITable table : tables) {
            for (int i = 0; i < table.getRowCount(); i++) {
                List<Object> pkValue = getPkValue(table, i);

                if (distinctIds.add(pkValue)) {
                    List<Object> values = getValues(table, i, tableMetaData.getColumns());
                    defaultTable.addRow(values.toArray(new Object[0]));
                }
            }
        }

        return defaultTable;
    }

    private List<Object> getPkValue(ITable table, int row) throws DataSetException {
        ITableMetaData tableMetaData = table.getTableMetaData();
        Column[] primaryKeys = tableMetaData.getPrimaryKeys();
        return getValues(table, row, primaryKeys);
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
