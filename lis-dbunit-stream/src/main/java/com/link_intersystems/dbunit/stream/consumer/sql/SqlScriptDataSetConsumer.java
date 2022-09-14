package com.link_intersystems.dbunit.stream.consumer.sql;

import com.link_intersystems.dbunit.sql.statement.InsertSqlFactory;
import com.link_intersystems.sql.format.SqlFormatSettings;

import java.io.PrintWriter;
import java.io.Writer;

public class SqlScriptDataSetConsumer extends AbstractSqlScriptDataSetConsumer {

    private PrintWriter writer;
    private SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();


    public SqlScriptDataSetConsumer(Writer writer) {
        this(new InsertSqlFactory(), writer);
    }

    public SqlScriptDataSetConsumer(InsertSqlFactory insertSqlFactory, Writer writer) {
        super(insertSqlFactory);
        this.writer = new PrintWriter(writer);
    }


    @Override
    protected void addInsertSql(String insertSql) {
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
