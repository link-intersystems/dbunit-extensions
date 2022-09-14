package com.link_intersystems.dbunit.stream.producer.sql;

import com.link_intersystems.dbunit.database.DatabaseConnectionBorrower;
import com.link_intersystems.sql.io.SqlScript;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SqlScriptDataSetProducer implements IDataSetProducer {

    private Logger logger = LoggerFactory.getLogger(SqlScriptDataSetProducer.class);

    private IDataSetConsumer dataSetConsumer = new DefaultConsumer();

    private SqlScript sqlScript;
    private DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
    private DatabaseConnectionBorrower connectionBorrower;

    public SqlScriptDataSetProducer(DatabaseConnectionBorrower connectionBorrower, SqlScript sqlScript) {
        this.connectionBorrower = requireNonNull(connectionBorrower);
        this.sqlScript = requireNonNull(sqlScript);
    }

    public void setDatabaseDataSetProducerConfig(DatabaseDataSetProducerConfig config) {
        this.config = requireNonNull(config);
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) {
        this.dataSetConsumer = requireNonNull(consumer);
    }

    @Override
    public void produce() throws DataSetException {
        try {
            IDatabaseConnection databaseConnection = connectionBorrower.borrowConnection();
            DataSetException produceException = null;
            try {
                produce(databaseConnection);
            } catch (DataSetException e) {
                produceException = e;
            } finally {
                if (databaseConnection != null) {
                    try {
                        connectionBorrower.returnConnection(databaseConnection);
                    } catch (DatabaseUnitException e) {
                        if (produceException != null) {
                            logger.error("Exception while producing sql script", produceException);
                        }
                        throw new DataSetException("Unable to return borrowed database connection.", e);
                    }
                }
            }
        } catch (DatabaseUnitException e) {
            throw new DataSetException(e);
        }
    }

    protected void produce(IDatabaseConnection databaseConnection) throws DataSetException {

        executeSqlScript(databaseConnection);

        DatabaseDataSetProducer dataSetProducer = new DatabaseDataSetProducer(databaseConnection, config);
        dataSetProducer.setConsumer(dataSetConsumer);
        dataSetProducer.produce();

    }

    protected void executeSqlScript(IDatabaseConnection databaseConnection) throws DataSetException {
        try (Connection connection = databaseConnection.getConnection()) {
            sqlScript.execute(connection);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }
}
