package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.dbunit.sql.statement.InsertSqlBuilder;
import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.sql.format.SqlFormatter;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;

import java.util.Objects;

public abstract class AbstractSqlScriptDataSetConsumer extends DefaultConsumer {

    private SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
    private ITableMetaData currMetaData;
    private InsertSqlBuilder insertSqlBuilder;


    public AbstractSqlScriptDataSetConsumer(SqlDialect sqlDialect) {
        this(new InsertSqlBuilder(sqlDialect));
    }

    public AbstractSqlScriptDataSetConsumer(InsertSqlBuilder insertSqlBuilder) {
        this.insertSqlBuilder = insertSqlBuilder;
    }

    public SqlFormatSettings getSqlFormatSettings() {
        return sqlFormatSettings;
    }

    public void setSqlFormatSettings(SqlFormatSettings sqlFormatSettings) {
        this.sqlFormatSettings = Objects.requireNonNull(sqlFormatSettings);
    }

    public void setSchema(String schema) {
        insertSqlBuilder.setSchema(schema);
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        this.currMetaData = iTableMetaData;
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        String insertSql = insertSqlBuilder.createInsertSql(currMetaData, values);
        SqlFormatSettings sqlFormatSettings = getSqlFormatSettings();

        String formattedSql = formatSql(insertSql);
        insertRow(formattedSql);
    }

    protected abstract void insertRow(String insertRowSql) throws DataSetException;


    private String formatSql(String insertSql) {
        SqlFormatSettings sqlFormatSettings = getSqlFormatSettings();

        SqlFormatter sqlFormatter = sqlFormatSettings.getSqlFormatter();

        if (sqlFormatter != null) {
            insertSql = sqlFormatter.format(insertSql);
        }

        return insertSql;
    }

}
