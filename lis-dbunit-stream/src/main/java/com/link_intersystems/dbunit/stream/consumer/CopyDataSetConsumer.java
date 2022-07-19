package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.dataset.MergedDataSet;
import com.link_intersystems.dbunit.table.TableList;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DefaultConsumer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CopyDataSetConsumer extends DefaultConsumer {

    private TableList tableList;
    private DefaultTable copyTable;
    private IDataSet dataSet;

    public IDataSet getDataSet() {
        return dataSet;
    }

    @Override
    public void startDataSet() throws DataSetException {
        tableList = new TableList();
    }

    @Override
    public void startTable(ITableMetaData metaData) throws DataSetException {
        ITableMetaData copyMetaData = copyMetaData(metaData);

        copyTable = new DefaultTable(copyMetaData);
    }

    protected DefaultTableMetaData copyMetaData(ITableMetaData metaData) throws DataSetException {
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

    @Override
    public void row(Object[] values) throws DataSetException {
        copyTable.addRow(values);
    }

    @Override
    public void endTable() {
        tableList.add(copyTable);
    }

    @Override
    public void endDataSet() throws DataSetException {
        MergedDataSet mergedDataSet = new MergedDataSet(tableList);
        endDataSet(mergedDataSet);
    }

    protected void endDataSet(IDataSet dataSet) throws DataSetException {
        this.dataSet = dataSet;
    }

}
