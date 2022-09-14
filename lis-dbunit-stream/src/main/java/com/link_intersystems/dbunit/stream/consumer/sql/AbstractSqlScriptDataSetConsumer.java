package com.link_intersystems.dbunit.stream.consumer.sql;

import com.link_intersystems.dbunit.sql.statement.InsertSqlFactory;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.sql.format.SqlFormatter;
import com.link_intersystems.sql.statement.InsertSql;
import com.link_intersystems.sql.statement.TableLiteralFormat;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;

import static java.util.Objects.requireNonNull;

public abstract class AbstractSqlScriptDataSetConsumer extends DefaultConsumer {

    private SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
    private ITableMetaData tableMetaData;
    private InsertSqlFactory insertSqlFactory;
    private TableLiteralFormatResolver tableLiteralFormatResolver = new DefaultTableLiteralFormatResolver();

    private TableLiteralFormat tableLiteralFormat;

    public AbstractSqlScriptDataSetConsumer(InsertSqlFactory insertSqlFactory) {
        this.insertSqlFactory = requireNonNull(insertSqlFactory);
    }

    public SqlFormatSettings getSqlFormatSettings() {
        return sqlFormatSettings;
    }

    public void setSqlFormatSettings(SqlFormatSettings sqlFormatSettings) {
        this.sqlFormatSettings = requireNonNull(sqlFormatSettings);
    }

    public void setTableLiteralFormatResolver(TableLiteralFormatResolver tableLiteralFormatResolver) {
        this.tableLiteralFormatResolver = requireNonNull(tableLiteralFormatResolver);
    }

    public void setSchema(String schema) {
        insertSqlFactory.setSchema(schema);
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        this.tableMetaData = iTableMetaData;
        tableLiteralFormat = tableLiteralFormatResolver.getTableLiteralFormat(tableMetaData);
    }

    @Override
    public void row(Object[] values) throws DataSetException {
        InsertSql insertSql = insertSqlFactory.createInsertSql(tableMetaData, values);

        try {
            String sql = insertSql.toSqlString(tableLiteralFormat);
            String statementDelimiter = sqlFormatSettings.getStatementDelimiter();
            String formattedSql = formatSql(sql + statementDelimiter);
            addInsertSql(formattedSql);
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    protected String formatSql(String insertSql) {
        SqlFormatSettings sqlFormatSettings = getSqlFormatSettings();

        SqlFormatter sqlFormatter = sqlFormatSettings.getSqlFormatter();

        if (sqlFormatter != null) {
            insertSql = sqlFormatter.format(insertSql);
        }

        return insertSql;
    }

    protected abstract void addInsertSql(String insertSql) throws DataSetException;

    @Override
    public void endTable() throws DataSetException {
        super.endTable();

        tableMetaData = null;
        tableLiteralFormat = null;
    }
}
