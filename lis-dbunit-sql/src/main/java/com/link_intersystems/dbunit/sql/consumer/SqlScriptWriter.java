package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.dbunit.sql.statement.InsertSqlBuilder;
import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.sql.format.SqlFormatter;
import org.dbunit.dataset.ITableMetaData;

import java.io.PrintWriter;
import java.io.Writer;

public class SqlScriptWriter extends AbstractSqlScriptDataSetConsumer {

    private PrintWriter writer;
    private SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();


    public SqlScriptWriter(SqlDialect sqlDialect, Writer writer) {
        this(new InsertSqlBuilder(sqlDialect), writer);
    }

    public SqlScriptWriter(InsertSqlBuilder insertSqlBuilder, Writer writer) {
        super(insertSqlBuilder);
        this.writer = new PrintWriter(writer);
    }


    @Override
    protected void insertRow(String insertSql) {
        writer.append(insertSql);

        String statementSeparator = sqlFormatSettings.getStatementSeparator();
        writer.append(statementSeparator);

        writer.flush();
    }

    @Override
    public void endDataSet() {
        writer.flush();
        writer.close();
    }

}
