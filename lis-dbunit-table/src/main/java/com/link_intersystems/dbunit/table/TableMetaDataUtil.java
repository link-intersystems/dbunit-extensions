package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableMetaDataUtil {

    private ITableMetaData metaData;

    private Map<String, Column> columnsByName;

    private Map<String, Column> pksByName;

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
        return primaryKeysByName.values().stream().toArray(Column[]::new);
    }
}
