package com.link_intersystems.dbunit.dataset.producer;

import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SmartDataSetProducerAdapter implements IDataSetProducer {

    private static interface TableIteratorSupplier {

        public ITableIterator get() throws DataSetException;
    }

    private static final Logger logger = LoggerFactory.getLogger(SmartDataSetProducerAdapter.class);
    private static final IDataSetConsumer EMPTY_CONSUMER = new DefaultConsumer();
    private IDataSetConsumer _consumer;
    private TableIteratorSupplier tableIteratorSupplier;

    public SmartDataSetProducerAdapter(ITableIterator iterator) {
        this._consumer = EMPTY_CONSUMER;
        this.tableIteratorSupplier = () -> iterator;
    }

    public SmartDataSetProducerAdapter(IDataSet dataSet) {
        this._consumer = EMPTY_CONSUMER;
        this.tableIteratorSupplier = () -> dataSet.iterator();
    }

    public void setConsumer(IDataSetConsumer consumer) {
        logger.debug("setConsumer(consumer) - start");
        this._consumer = consumer;
    }

    public void produce() throws DataSetException {
        ITableIterator tableIterator = tableIteratorSupplier.get();
        logger.debug("produce() - start");
        this._consumer.startDataSet();

        while (tableIterator.next()) {
            ITable table = tableIterator.getTable();
            ITableMetaData metaData = table.getTableMetaData();
            this._consumer.startTable(metaData);

            try {
                Column[] columns = metaData.getColumns();
                if (columns.length != 0) {
                    int i = 0;

                    while (true) {
                        Object[] values = new Object[columns.length];

                        for (int j = 0; j < columns.length; ++j) {
                            Column column = columns[j];
                            values[j] = table.getValue(i, column.getColumnName());
                        }

                        this._consumer.row(values);
                        ++i;
                    }
                }

                this._consumer.endTable();
            } catch (RowOutOfBoundsException var8) {
                this._consumer.endTable();
            }
        }

        this._consumer.endDataSet();
    }
}
