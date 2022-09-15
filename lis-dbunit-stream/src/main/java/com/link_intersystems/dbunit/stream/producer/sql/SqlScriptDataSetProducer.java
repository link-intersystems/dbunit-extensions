package com.link_intersystems.dbunit.stream.producer.sql;

import com.link_intersystems.dbunit.database.DatabaseConnectionPool;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
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
    private DatabaseConnectionPool connectionPool;

    public SqlScriptDataSetProducer(DatabaseConnectionPool connectionPool, SqlScript sqlScript) {
        this.connectionPool = requireNonNull(connectionPool);
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
            IDatabaseConnection databaseConnection = connectionPool.borrowConnection();
            try {
                produce(databaseConnection);
            } catch (DataSetException e) {
                logger.error("Exception while producing sql script", e);
                throw e;
            } finally {
                if (databaseConnection != null) {
                    connectionPool.returnConnection(databaseConnection);
                }
            }
        } catch (DataSetException e) {
            throw e;
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
