package com.link_intersystems.dbunit.dataset.producer;

import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.csv.CsvProducer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.dbunit.dataset.xml.XmlProducer;
import org.xml.sax.InputSource;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetProducerSupport {

    // CsvProducer

    default public void setCsvProducer(String inputDirectory) {
        setCsvProducer(new File(inputDirectory));
    }

    default public void setCsvProducer(File inputDirectory) {
        setDataSetProducer(new CsvProducer(inputDirectory));
    }


    // XlsProducer

    default public void setXlsProducer(String file) throws IOException {
        setXlsProducer(new File(file));
    }

    default public void setXlsProducer(File file) throws IOException {
        setXlsProducer(new FileInputStream(file));
    }

    default public void setXlsProducer(InputStream inputStream) throws IOException {
        setDataSetProducer(new XlsDataSetProducer(new BufferedInputStream(inputStream)));
    }


    // XmlProducer

    default public void setXmlProducer(String file) throws IOException {
        setXmlProducer(new File(file));
    }

    default public void setXmlProducer(File file) throws IOException {
        setXmlProducer(new FileInputStream(file));
    }

    default public void setXmlProducer(InputStream inputStream) {
        setXmlProducer(inputStream, StandardCharsets.UTF_8);
    }

    default public void setXmlProducer(InputStream inputStream, Charset charset) {
        setDataSetProducer(new XmlProducer(new InputSource(new InputStreamReader(new BufferedInputStream(inputStream), charset))));
    }

    // FlatXmlProducer

    default public void setFlatXmlProducer(String file) throws IOException {
        setFlatXmlProducer(new File(file));
    }

    default public void setFlatXmlProducer(File file) throws IOException {
        setFlatXmlProducer(new FileInputStream(file));
    }

    default public void setFlatXmlProducer(InputStream inputStream) {
        setFlatXmlProducer(inputStream, StandardCharsets.UTF_8);
    }

    default public void setFlatXmlProducer(InputStream inputStream, Charset charset) {
        setDataSetProducer(new FlatXmlProducer(new InputSource(new InputStreamReader(new BufferedInputStream(inputStream), charset))));
    }

    //

    default public void setDataSetProducer(IDataSet dataSet) {
        setDataSetProducer(new DataSetSourceProducerAdapter(dataSet));
    }

    public void setDataSetProducer(IDataSetProducer dataSetProducer);

}
