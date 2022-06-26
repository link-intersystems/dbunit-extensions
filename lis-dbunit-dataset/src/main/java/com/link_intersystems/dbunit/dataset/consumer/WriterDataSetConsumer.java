package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.swing.text.TextTable;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class WriterDataSetConsumer implements IDataSetConsumer {

    private Writer writer;
    private final String lineSeparator;

    private TextTable textTable;
    private DefaultTableModel tableModel;

    /**
     * Writers to {@link System#out}.
     */
    public WriterDataSetConsumer() {
        this(new PrintWriter(System.out));
    }

    public WriterDataSetConsumer(Writer writer) {
        this.writer = requireNonNull(writer);
        this.lineSeparator = System.lineSeparator();
    }

    @Override
    public void startDataSet() {
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void startTable(ITableMetaData tableMetaData) throws DataSetException {
        Column[] columns = tableMetaData.getColumns();

        tableModel = new DefaultTableModel(Arrays.stream(columns).map(Column::getColumnName).toArray(String[]::new), 1);
        textTable = createTextTable(tableMetaData, tableModel);
    }


    @Override
    public void endTable() throws DataSetException {
        try {
            textTable.print(writer);
            writer.write(lineSeparator);
            writer.write(lineSeparator);
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void row(Object[] objects) {
        tableModel.addRow(objects);
    }

    protected TextTable createTextTable(ITableMetaData tableMetaData, TableModel tableModel) {
        String tableName = tableMetaData.getTableName();
        TextTable textTable = new TextTable(tableModel);
        textTable.setTitle(tableName);
        return textTable;
    }
}
