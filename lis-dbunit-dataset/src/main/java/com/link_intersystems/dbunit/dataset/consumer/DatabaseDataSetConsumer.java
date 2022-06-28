package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.dbunit.sql.consumer.AbstractSqlScriptDataSetConsumer;
import com.link_intersystems.dbunit.sql.statement.InsertSqlBuilder;
import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.dialect.SqlDialect;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDataSetConsumer extends AbstractSqlScriptDataSetConsumer {

    private Connection connection;
    private Statement tableStatement;
    private boolean commitAtEnd = true;

    public DatabaseDataSetConsumer(Connection connection) {
        this(connection, new DefaultSqlDialect());
    }

    public DatabaseDataSetConsumer(Connection connection, SqlDialect sqlDialect) {
        this(connection, new InsertSqlBuilder(sqlDialect));
    }

    public DatabaseDataSetConsumer(Connection connection, InsertSqlBuilder insertSqlBuilder) {
        super(insertSqlBuilder);
        this.connection = requireNonNull(connection);
    }

    public void setCommitAtEnd(boolean commitAtEnd) {
        this.commitAtEnd = commitAtEnd;
    }

    public boolean isCommitAtEnd() {
        return commitAtEnd;
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        super.startTable(iTableMetaData);

        try {
            tableStatement = connection.createStatement();
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    protected void insertRow(String insertRowSql) throws DataSetException {
        try {
            tableStatement.addBatch(insertRowSql);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void endTable() throws DataSetException {
        try {
            tableStatement.executeBatch();
            tableStatement.close();
            tableStatement = null;
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        super.endDataSet();
        if (isCommitAtEnd()) {
            commit();
        }
    }

    private void commit() throws DataSetException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("COMMIT");
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }
}
