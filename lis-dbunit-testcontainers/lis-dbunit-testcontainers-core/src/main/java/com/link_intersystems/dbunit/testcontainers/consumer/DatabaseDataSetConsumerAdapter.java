package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.RowFilterConsumer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetConsumerAdapter extends DefaultContainerAwareDataSetConsumer implements ChainableDataSetConsumer {

    private IRowFilterFactory rowFilterFactory;

    public void setRowFilterFactory(IRowFilterFactory rowFilterFactory) {
        this.rowFilterFactory = rowFilterFactory;
    }

    @Override
    protected void startDataSet(JdbcContainer jdbcContainer) {
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) {
    }

    @Override
    public void row(Object[] objects) {
    }

    @Override
    public void endTable() {
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            IDatabaseConnection databaseConnection = getJdbcContainer().getDatabaseConnection();
            processResult(databaseConnection, getDelegate());
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    protected void processResult(IDatabaseConnection databaseConnection, IDataSetConsumer resultConsumer) throws SQLException, DataSetException {
        DatabaseDataSet databaseDataSet = createDataSet(databaseConnection);
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(databaseDataSet);

        IDataSetConsumer effectiveConsumer = resultConsumer;

        RowFilterConsumer rowFilterConsumer = new RowFilterConsumer();
        rowFilterConsumer.setRowFilterFactory(rowFilterFactory);
        rowFilterConsumer.setSubsequentConsumer(resultConsumer);
        effectiveConsumer = rowFilterConsumer;

        dataSetProducerAdapter.setConsumer(effectiveConsumer);
        dataSetProducerAdapter.produce();
    }

    protected DatabaseDataSet createDataSet(IDatabaseConnection databaseConnection) throws SQLException {
        return new DatabaseDataSet(databaseConnection, false);
    }
}
