package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.csv.CsvDataSetWriter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.dbunit.dataset.xml.XmlDataSetWriter;
import org.dbunit.operation.DatabaseOperation;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetConsumerSupport {

    // DatabaseConsumer
    default public void setDatabaseConsumer(Connection connection) throws DatabaseUnitException {
        setDatabaseConsumer(new DatabaseConnection(connection));
    }

    default public void setDatabaseConsumer(Connection connection, DatabaseOperation databaseOperation) throws DatabaseUnitException {
        setDatabaseConsumer(new DatabaseConnection(connection), databaseOperation);
    }

    default public void setDatabaseConsumer(IDatabaseConnection connection) {
        setDatabaseConsumer(connection, DatabaseOperation.INSERT);
    }

    default public void setDatabaseConsumer(IDatabaseConnection connection, DatabaseOperation databaseOperation) {
        setDataSetConsumer(new DatabaseDataSetConsumer(connection, databaseOperation));
    }


    // CsvConsumer

    default public void setCsvConsumer(String outputDirectory) {
        setCsvConsumer(new File(outputDirectory));
    }

    default public void setCsvConsumer(File outputDirectory) {
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

    default public void setDataSetConsumers(IDataSetConsumer... dataSetConsumers) {
        setDataSetConsumer(new RepeatingDataSetConsumer(dataSetConsumers));
    }

    default public void setDataSetConsumers(List<IDataSetConsumer> dataSetConsumers) {
        setDataSetConsumer(new RepeatingDataSetConsumer(dataSetConsumers));
    }

    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer);


}
