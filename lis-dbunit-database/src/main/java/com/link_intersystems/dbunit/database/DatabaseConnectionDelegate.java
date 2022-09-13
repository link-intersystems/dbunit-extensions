package com.link_intersystems.dbunit.database;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class DatabaseConnectionDelegate implements IDatabaseConnection {

    protected abstract IDatabaseConnection getTargetConnection();

    @Override
    public Connection getConnection() throws SQLException {
        return getTargetConnection().getConnection();
    }

    @Override
    public String getSchema() {
        return getTargetConnection().getSchema();
    }

    @Override
    public void close() throws SQLException {
        getTargetConnection().close();
    }

    @Override
    public IDataSet createDataSet() throws SQLException {
        return getTargetConnection().createDataSet();
    }

    @Override
    public IDataSet createDataSet(String[] tableNames) throws SQLException, DataSetException {
        return getTargetConnection().createDataSet(tableNames);
    }

    @Override
    public ITable createQueryTable(String tableName, String sql) throws DataSetException, SQLException {
        return getTargetConnection().createQueryTable(tableName, sql);
    }

    @Override
    public ITable createTable(String tableName, PreparedStatement preparedStatement) throws DataSetException, SQLException {
        return getTargetConnection().createTable(tableName, preparedStatement);
    }

    @Override
    public ITable createTable(String tableName) throws DataSetException, SQLException {
        return getTargetConnection().createTable(tableName);
    }

    @Override
    public int getRowCount(String tableName) throws SQLException {
        return getTargetConnection().getRowCount(tableName);
    }

    @Override
    public int getRowCount(String tableName, String whereClause) throws SQLException {
        return getTargetConnection().getRowCount(tableName, whereClause);
    }

    @Override
    public DatabaseConfig getConfig() {
        return getTargetConnection().getConfig();
    }

    @Override
    public IStatementFactory getStatementFactory() {
        return getTargetConnection().getStatementFactory();
    }
}
