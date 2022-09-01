package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.RowFilterConsumer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ReproduceConsumerAdapter extends JdbcContainerAwareDataSetConsumer implements ChainableDataSetConsumer {

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
        IDataSetProducer dataSetProducer = new DatabaseDataSetProducer(databaseConnection);

        RowFilterConsumer rowFilterConsumer = new RowFilterConsumer();
        rowFilterConsumer.setRowFilterFactory(rowFilterFactory);
        rowFilterConsumer.setSubsequentConsumer(resultConsumer);
        IDataSetConsumer effectiveConsumer = rowFilterConsumer;

        dataSetProducer.setConsumer(effectiveConsumer);
        dataSetProducer.produce();
    }
}
