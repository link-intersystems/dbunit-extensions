package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.dialect.SqlDialect;
import org.dbunit.dataset.csv.CsvDataSetWriter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.dbunit.dataset.xml.XmlDataSetWriter;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetConsumerSupport {

    // DatabaseConsumer
    default public void setDatabaseConsumer(String jdbcUrl, String username, String password) throws IOException {
        setDatabaseConsumer(jdbcUrl, username, password, new DefaultSqlDialect());
    }

    default public void setDatabaseConsumer(String jdbcUrl, String username, String password, SqlDialect sqlDialect) throws IOException {
        try {
            setDatabaseConsumer(DriverManager.getConnection(jdbcUrl, username, password), sqlDialect);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    default public void setDatabaseConsumer(Connection connection) throws IOException {
        setDatabaseConsumer(connection, new DefaultSqlDialect());
    }

    default public void setDatabaseConsumer(Connection connection, SqlDialect sqlDialect) throws IOException {
        setDataSetConsumer(new DatabaseDataSetConsumer(connection, sqlDialect));
    }


    // CsvConsumer

    default public void setCsvConsumer(String outputDirectory) throws IOException {
        setCsvConsumer(new File(outputDirectory));
    }

    default public void setCsvConsumer(File outputDirectory) throws IOException {
        setDataSetConsumer(new CsvDataSetWriter(outputDirectory));
    }


    // XlsConsumer

    default public void setXlsConsumer(String file) throws IOException {
        setXlsConsumer(new File(file));
    }

    default public void setXlsConsumer(File file) throws IOException {
        setXlsConsumer(new FileOutputStream(file));
    }

    default public void setXlsConsumer(OutputStream outputStream) {
        setDataSetConsumer(new XlsDataSetConsumer(new BufferedOutputStream(outputStream)));
    }


    // XmlConsumer

    default public void setXmlConsumer(String file) throws IOException {
        setXmlConsumer(new File(file));
    }

    default public void setXmlConsumer(File file) throws IOException {
        setXmlConsumer(new FileOutputStream(file));
    }

    default public void setXmlConsumer(OutputStream outputStream) {
        setXmlConsumer(outputStream, StandardCharsets.UTF_8);
    }

    default public void setXmlConsumer(OutputStream outputStream, Charset charset) {
        setDataSetConsumer(new XmlDataSetWriter(new OutputStreamWriter(new BufferedOutputStream(outputStream), charset)));
    }

    // FlatXmlConsumer

    default public void setFlatXmlConsumer(String file) throws IOException {
        setFlatXmlConsumer(new File(file));
    }

    default public void setFlatXmlConsumer(File file) throws IOException {
        setFlatXmlConsumer(new FileOutputStream(file));
    }

    default public void setFlatXmlConsumer(OutputStream outputStream) {
        setFlatXmlConsumer(outputStream, StandardCharsets.UTF_8);
    }

    default public void setFlatXmlConsumer(OutputStream outputStream, Charset charset) {
        setDataSetConsumer(new FlatXmlWriter(new OutputStreamWriter(new BufferedOutputStream(outputStream), charset)));
    }

    //

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer);

}
