package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.dbunit.sql.statement.InsertSqlBuilder;
import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.sql.format.SqlFormatter;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;

import java.io.PrintWriter;
import java.io.Writer;
import java.util.Objects;

public class SqlScriptWriter extends DefaultConsumer {

    private PrintWriter writer;
    private SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
    private ITableMetaData currMetaData;
    private InsertSqlBuilder insertSqlBuilder;


    public SqlScriptWriter(SqlDialect sqlDialect, Writer writer) {
        this(new InsertSqlBuilder(sqlDialect), writer);
    }

    public SqlScriptWriter(InsertSqlBuilder insertSqlBuilder, Writer writer) {
        this.insertSqlBuilder = insertSqlBuilder;
        this.writer = new PrintWriter(writer);
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
    public void startDataSet() {
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) {
        this.currMetaData = iTableMetaData;
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        String insertSql = insertSqlBuilder.createInsertSql(currMetaData, values);
        SqlFormatSettings sqlFormatSettings = getSqlFormatSettings();

        writer.append(formatSql(insertSql));

        String statementSeparator = sqlFormatSettings.getStatementSeparator();
        writer.append(statementSeparator);

        writer.flush();
    }

    private String formatSql(String insertSql) {
        SqlFormatSettings sqlFormatSettings = getSqlFormatSettings();

        SqlFormatter sqlFormatter = sqlFormatSettings.getSqlFormatter();

        if (sqlFormatter != null) {
            insertSql = sqlFormatter.format(insertSql);
        }

        return insertSql;
    }

    @Override
    public void endDataSet() {
        writer.flush();
        writer.close();
    }

}
