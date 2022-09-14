package com.link_intersystems.dbunit.stream.resource.sql;

import com.link_intersystems.dbunit.database.DatabaseConnectionBorrower;
import com.link_intersystems.dbunit.stream.consumer.sql.DefaultTableLiteralFormatResolver;
import com.link_intersystems.dbunit.stream.consumer.sql.SqlScriptDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.sql.TableLiteralFormatResolver;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
import com.link_intersystems.dbunit.stream.producer.sql.SqlScriptDataSetProducer;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.sql.io.SqlScript;
import com.link_intersystems.sql.io.URLScriptResource;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SqlDataSetFile implements DataSetFile {

    private File dataScript;
    private Charset charset = StandardCharsets.UTF_8;
    private DatabaseDataSetProducerConfig databaseDataSetProducerConfig = new DatabaseDataSetProducerConfig();
    private TableLiteralFormatResolver tableLiteralFormatResolver = new DefaultTableLiteralFormatResolver();
    private DatabaseConnectionBorrower databaseConnectionBorrower;


    public SqlDataSetFile(File sqlScript) {
        this.dataScript = requireNonNull(sqlScript);
    }

    private SqlDataSetFile(SqlDataSetFile sqlDataSetFile) {
        dataScript = sqlDataSetFile.dataScript;
        charset = sqlDataSetFile.charset;
        databaseDataSetProducerConfig = sqlDataSetFile.databaseDataSetProducerConfig;
        databaseConnectionBorrower = sqlDataSetFile.databaseConnectionBorrower;
    }

    public void setTableLiteralFormatResolver(TableLiteralFormatResolver tableLiteralFormatResolver) {
        this.tableLiteralFormatResolver = requireNonNull(tableLiteralFormatResolver);
    }

    public void setCharset(Charset charset) {
        this.charset = requireNonNull(charset);
    }

    public void setDatabaseConnectionBorrower(DatabaseConnectionBorrower databaseConnectionBorrower) {
        this.databaseConnectionBorrower = databaseConnectionBorrower;
    }

    public void setDatabaseDataSetProducerConfig(DatabaseDataSetProducerConfig databaseDataSetProducerConfig) {
        this.databaseDataSetProducerConfig = databaseDataSetProducerConfig;
    }

    @Override
    public IDataSetProducer createProducer() throws DataSetException {
        if (databaseConnectionBorrower == null) {
            String msg = MessageFormat.format("Unable to create producer, because no {0} is set", DatabaseConnectionBorrower.class.getSimpleName());
            throw new DataSetException(msg);
        }

        URLScriptResource dataResource = URLScriptResource.fromFile(this.dataScript);
        dataResource.setCharset(charset);
        SqlScript dataScript = new SqlScript(dataResource);

        SqlScriptDataSetProducer scriptDataSetProducer = new SqlScriptDataSetProducer(databaseConnectionBorrower, dataScript);
        scriptDataSetProducer.setDatabaseDataSetProducerConfig(databaseDataSetProducerConfig);

        return scriptDataSetProducer;
    }

    @Override
    public IDataSetConsumer createConsumer() throws DataSetException {
        try {
            FileOutputStream outputStream = new FileOutputStream(dataScript);
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, charset);
            Writer writer = new BufferedWriter(outputStreamWriter);
            SqlScriptDataSetConsumer sqlScriptDataSetConsumer = new SqlScriptDataSetConsumer(writer);
            String schema = databaseDataSetProducerConfig.getSchema();
            sqlScriptDataSetConsumer.setSchema(schema);
            sqlScriptDataSetConsumer.setTableLiteralFormatResolver(tableLiteralFormatResolver);
            return sqlScriptDataSetConsumer;
        } catch (FileNotFoundException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public DataSetFile withNewFile(File file) {
        File parentFile = file.getParentFile();
        parentFile.mkdirs();
        SqlDataSetFile sqlDataSetFile = new SqlDataSetFile(this);
        sqlDataSetFile.dataScript = file;
        return sqlDataSetFile;
    }

    @Override
    public File getFile() {
        return dataScript;
    }

    @Override
    public String toString() {
        return getFile().toString();
    }

}
