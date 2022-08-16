package com.link_intersystems.dbunit.stream.resource.db;

import com.link_intersystems.dbunit.stream.consumer.DatabaseDataSetConsumer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.operation.DatabaseOperation;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetResource implements DataSetResource {
    private IDatabaseConnection databaseConnection;
    private DatabaseOperation consumerDatabaseOperation = DatabaseOperation.INSERT;

    private DatabaseDataSetProducerConfig databaseDataSetProducerConfig = new DatabaseDataSetProducerConfig();

    public DatabaseDataSetResource(IDatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
    }

    public void setDatabaseDataSetProducerConfig(DatabaseDataSetProducerConfig databaseDataSetProducerConfig) {
        this.databaseDataSetProducerConfig = Objects.requireNonNull(databaseDataSetProducerConfig);
    }

    public void setConsumerDatabaseOperation(DatabaseOperation consumerDatabaseOperation) {
        this.consumerDatabaseOperation = requireNonNull(consumerDatabaseOperation);
    }

    @Override
    public IDataSetProducer createProducer() throws DataSetException {
        DatabaseDataSetProducer databaseDataSetProducer = new DatabaseDataSetProducer(databaseConnection, databaseDataSetProducerConfig);
        databaseDataSetProducer.setDatabaseDataSetProducerConfig(databaseDataSetProducerConfig);
        return databaseDataSetProducer;
    }

    @Override
    public IDataSetConsumer createConsumer() throws DataSetException {
        return new DatabaseDataSetConsumer(databaseConnection, consumerDatabaseOperation);
    }
}
