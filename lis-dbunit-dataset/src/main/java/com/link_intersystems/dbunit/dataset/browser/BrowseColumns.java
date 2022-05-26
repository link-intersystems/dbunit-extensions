package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.meta.TableReference;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BrowseColumns  {

    private TableMetaDataRepository tableMetaDataRepository;
    private String sourceTableName;
    private String[] sourceColumns;
    private String targetTableName;
    private String[] targetColumns;

    public BrowseColumns(TableMetaDataRepository tableMetaDataRepository, String sourceTableName, String[] sourceColumns, String targetTableName, String[] targetColumns) {
        this.tableMetaDataRepository = tableMetaDataRepository;
        this.sourceTableName = sourceTableName;
        this.sourceColumns = sourceColumns;
        this.targetTableName = targetTableName;
        this.targetColumns = targetColumns;
    }

    public TableReference.Edge getTargetEdge() throws DataSetException {
        try {
            return toEdge(targetTableName, Arrays.asList(targetColumns));
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Unable to resolve target edge", e);
        }
    }

    public TableReference.Edge getSourceEdge() throws DataSetException {
        try {
            return toEdge(sourceTableName, Arrays.asList(sourceColumns));
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Unable to resolve source edge", e);
        }
    }


    private TableReference.Edge toEdge(String tableName, List<String> columnNames) throws DataSetException {
        ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);

        List<Column> columns = Arrays.stream(tableMetaData.getColumns()).filter(c -> columnNames.contains(c.getColumnName())).collect(Collectors.toList());

        if (columns.size() != columnNames.size()) {
            List<String> tableColumnNames = Arrays.stream(tableMetaData.getColumns()).map(Column::getColumnName).collect(Collectors.toList());
            String msg = MessageFormat.format("Columns {1} are not contained in the table {0} with the columns {2}", tableName, columnNames, tableColumnNames);
            throw new IllegalStateException(msg);
        }

        return new TableReference.Edge(tableMetaData, columns);
    }
}
