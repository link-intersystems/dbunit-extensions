package com.link_intersystems.dbunit.stream.producer.sql;

import com.link_intersystems.dbunit.table.Row;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.jdbc.TableMetaData;
import org.dbunit.database.*;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.util.QualifiedTableName;
import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * A streaming {@link IDataSetProducer} for database connections.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetProducer implements IDataSetProducer {

    private Logger logger = LoggerFactory.getLogger(DatabaseDataSetProducer.class);

    private DatabaseDataSetProducerConfig databaseDataSetProducerConfig;
    private IDatabaseConnection connection;
    private final ITableFilterSimple oracleRecycleBinTableFilter;
    private OrderedTableNameMap tableMap;

    public DatabaseDataSetProducer(IDatabaseConnection connection) {
        this(connection, new DatabaseDataSetProducerConfig());
    }

    public DatabaseDataSetProducer(IDatabaseConnection databaseConnection, DatabaseDataSetProducerConfig databaseDataSetProducerConfig) {
        this.connection = requireNonNull(databaseConnection);
        this.databaseDataSetProducerConfig = requireNonNull(databaseDataSetProducerConfig);
        oracleRecycleBinTableFilter = new OracleRecycleBinTableFilter(connection.getConfig());
    }

    public void setDatabaseDataSetProducerConfig(DatabaseDataSetProducerConfig databaseDataSetProducerConfig) {
        this.databaseDataSetProducerConfig = requireNonNull(databaseDataSetProducerConfig);
    }

    protected String getSchema() {
        String schema = databaseDataSetProducerConfig.getSchema();
        if (schema == null) {
            schema = getDefaultSchema();
        }
        return schema;
    }

    private IDataSetConsumer dataSetConsumer = new DefaultConsumer();

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        dataSetConsumer = requireNonNull(consumer);
    }

    @Override
    public void produce() throws DataSetException {
        String[] tableNames = getTableNames();

        dataSetConsumer.startDataSet();

        for (String tableName : tableNames) {
            ITableMetaData tableMetaData = getTableMetaData(tableName);
            dataSetConsumer.startTable(tableMetaData);

            try {
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, connection);
                TableUtil tableUtil = new TableUtil(forwardOnlyResultSetTable);
                try {
                    int i = 0;
                    while (i < Integer.MAX_VALUE) {
                        Row row = tableUtil.getRow(i++);
                        dataSetConsumer.row(row.toArray());
                    }
                } catch (RowOutOfBoundsException e) {
                    dataSetConsumer.endTable();
                }
            } catch (SQLException e) {
                throw new DataSetException(e);
            }
        }

        dataSetConsumer.endDataSet();
    }

    public String[] getTableNames() throws DataSetException {
        if (tableMap == null) {
            tableMap = loadTableMap();
        }

        return tableMap.getTableNames();
    }


    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        logger.debug("getTableMetaData(tableName={}) - start", tableName);

        OrderedTableNameMap tableMap = loadTableMap();

        if (!tableMap.containsTable(tableName)) {
            logger.error("Table '{}' not found in tableMap={}", tableName, tableMap);
            throw new NoSuchTableException(tableName);
        }

        ITableMetaData metaData = (ITableMetaData) tableMap.get(tableName);
        if (metaData != null) {
            return metaData;
        }

        metaData = createDatabaseTableMetaData(connection, tableName);
        tableMap.update(tableName, metaData);

        return metaData;
    }

    @SuppressWarnings("deprecation")
    protected ITableMetaData createDatabaseTableMetaData(IDatabaseConnection connection, String tableName) throws DataSetException {
        return new DatabaseTableMetaDataAccess(tableName, connection, true, isCaseSensitiveTableNames());
    }

    protected String getDefaultSchema() {
        return connection.getSchema();
    }

    protected OrderedTableNameMap loadTableMap() throws DataSetException {
        OrderedTableNameMap tableMap = new OrderedTableNameMap(isCaseSensitiveTableNames());
        DatabaseConfig config = connection.getConfig();

        String schema = getSchema();

        try {
            ResultSet resultSet = loadMetaData(config, schema);

            if (logger.isDebugEnabled()) {
                Connection jdbcConnection2 = connection.getConnection();
                logger.debug(SQLHelper.getDatabaseInfo(jdbcConnection2.getMetaData()));
                logger.debug("metadata resultset={}", resultSet);
            }

            ITableFilterSimple tableFilter = databaseDataSetProducerConfig.getTableFilter();

            try {
                IMetadataHandler metadataHandler2 = (IMetadataHandler) config.getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);
                while (resultSet.next()) {
                    String schemaName = metadataHandler2.getSchema(resultSet);

                    TableMetaData tableMetaData = new TableMetaData(resultSet);
                    String tableName = tableMetaData.getTableName();

                    if (!tableFilter.accept(tableName)) {
                        logger.debug("Skipping table '{}'", tableName);
                        continue;
                    }
                    if (!oracleRecycleBinTableFilter.accept(tableName)) {
                        logger.debug("Skipping oracle recycle bin table '{}'", tableName);
                        continue;
                    }

                    QualifiedTableName qualifiedTableName = new QualifiedTableName(tableName, schemaName);
                    tableName = qualifiedTableName.getQualifiedNameIfEnabled(config);

                    // Put the table into the table map
                    tableMap.add(tableName, null);
                }
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }

        return tableMap;
    }

    private ResultSet loadMetaData(DatabaseConfig config, String schema) throws SQLException {
        Connection jdbcConnection = connection.getConnection();
        DatabaseMetaData databaseMetaData = jdbcConnection.getMetaData();

        if (SQLHelper.isSybaseDb(jdbcConnection.getMetaData()) && !jdbcConnection.getMetaData().getUserName().equals(schema)) {
            logger.warn("For sybase the schema name should be equal to the user name. " + "Otherwise the DatabaseMetaData#getTables() method might not return any columns. " + "See dbunit tracker #1628896 and http://issues.apache.org/jira/browse/TORQUE-40?page=all");
        }

        String[] tableType = (String[]) config.getProperty(DatabaseConfig.PROPERTY_TABLE_TYPE);
        IMetadataHandler metadataHandler = (IMetadataHandler) config.getProperty(DatabaseConfig.PROPERTY_METADATA_HANDLER);

        ResultSet resultSet = metadataHandler.getTables(databaseMetaData, schema, tableType);
        return resultSet;
    }

    private boolean isCaseSensitiveTableNames() {
        return databaseDataSetProducerConfig.isCaseSensitiveTableNames();
    }


    private static class OracleRecycleBinTableFilter implements ITableFilterSimple {
        private final DatabaseConfig _config;

        public OracleRecycleBinTableFilter(DatabaseConfig config) {
            this._config = config;
        }

        public boolean accept(String tableName) {
            // skip oracle 10g recycle bin system tables if enabled
            if (_config.getFeature(DatabaseConfig.FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES)) {
                // Oracle 10g workaround
                // don't process system tables (oracle recycle bin tables) which
                // are reported to the application due a bug in the oracle JDBC driver
                if (tableName.startsWith("BIN$")) {
                    return false;
                }
            }

            return true;
        }
    }
}
