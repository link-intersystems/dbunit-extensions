package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableMetaDataUtil {

    private ITableMetaData metaData;
    private Map<String, Column> columnsByName;
    private Map<String, Column> pksByName;
    private List<String> columnNames;

    public TableMetaDataUtil(ITableMetaData metaData) {
        this.metaData = requireNonNull(metaData);
    }

    public Column getColumn(String columnName) throws DataSetException {
        Map<String, Column> columnsByName = getColumns();
        return columnsByName.get(columnName);
    }

    private Map<String, Column> getColumns() throws DataSetException {
        if (columnsByName == null) {
            columnsByName = new LinkedHashMap<>();

            Column[] columns = metaData.getColumns();
            Arrays.stream(columns).forEach(c -> columnsByName.put(c.getColumnName(), c));
        }
        return columnsByName;
    }

    private Map<String, Column> getPrimaryKeysByName() throws DataSetException {
        if (pksByName == null) {
            pksByName = new LinkedHashMap<>();

            Column[] columns = metaData.getPrimaryKeys();
            Arrays.stream(columns).forEach(c -> pksByName.put(c.getColumnName(), c));
        }
        return pksByName;
    }

    public boolean isPrimaryKey(String columnName) throws DataSetException {
        Map<String, Column> primaryKeys = getPrimaryKeysByName();
        return primaryKeys.containsKey(columnName);
    }

    public Column[] getPrimaryKeys() throws DataSetException {
        Map<String, Column> primaryKeysByName = getPrimaryKeysByName();
        return primaryKeysByName.values().toArray(new Column[0]);
    }

    private List<String> getColumnNames() throws DataSetException {
        if (columnNames == null) {
            columnNames = Arrays.stream(metaData.getColumns()).map(Column::getColumnName).collect(Collectors.toList());
        }
        return columnNames;
    }

    public int indexOf(String columnName) throws DataSetException {
        return getColumnNames().indexOf(columnName);
    }

    public ITableMetaData copy(ITableMetaData metaData) throws DataSetException {
        String tableName = metaData.getTableName();
        Column[] columns = copyColumns(metaData.getColumns());
        Column[] primaryKeys = getPrimaryKeys(columns, metaData.getPrimaryKeys());
        return new DefaultTableMetaData(tableName, columns, primaryKeys);
    }

    private Column[] getPrimaryKeys(Column[] columns, Column[] primaryKeys) {
        List<String> pkColNames = Arrays.stream(primaryKeys).map(Column::getColumnName).collect(Collectors.toList());
        return Arrays.stream(columns).filter(c -> pkColNames.contains(c.getColumnName())).toArray(Column[]::new);
    }

    private Column[] copyColumns(Column[] columns) {
        return Arrays.stream(columns)
                .map(c -> new Column(
                                c.getColumnName(),
                                c.getDataType(),
                                c.getSqlTypeName(),
                                c.getNullable(),
                                c.getDefaultValue(),
                                c.getRemarks(),
                                c.getAutoIncrement()
                        )
                ).toArray(Column[]::new);
    }


}
