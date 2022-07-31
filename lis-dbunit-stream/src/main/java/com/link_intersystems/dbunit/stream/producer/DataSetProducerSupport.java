package com.link_intersystems.dbunit.stream.producer;

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
        CsvProducer dataSetProducer = new CsvProducer(inputDirectory);
        AutocloseDataSetProducer closeableDataSetProducer = new AutocloseDataSetProducer(dataSetProducer);
        setDataSetProducer(closeableDataSetProducer);
    }


    // XlsProducer

    default public void setXlsProducer(String file) throws IOException {
        setXlsProducer(new File(file));
    }

    default public void setXlsProducer(File file) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        XlsDataSetProducer dataSetProducer = new XlsDataSetProducer(new BufferedInputStream(inputStream));
        setDataSetProducer(new AutocloseDataSetProducer(dataSetProducer, inputStream));
    }

    default public void setXlsProducer(InputStream inputStream) throws IOException {
        XlsDataSetProducer dataSetProducer = new XlsDataSetProducer(new BufferedInputStream(inputStream));
        setDataSetProducer(dataSetProducer);
    }


    // XmlProducer

    default public void setXmlProducer(String file) throws IOException {
        setXmlProducer(new File(file));
    }

    default public void setXmlProducer(File file) throws IOException {
        setXmlProducer(file, StandardCharsets.UTF_8);
    }

    default public void setXmlProducer(File file, Charset charset) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        InputStreamReader reader = new InputStreamReader(bufferedInputStream, charset);
        InputSource inputSource = new InputSource(reader);
        XmlProducer xmlProducer = new XmlProducer(inputSource);
        setDataSetProducer(new AutocloseDataSetProducer(xmlProducer, inputStream));
    }

    default public void setXmlProducer(InputStream inputStream) {
        setXmlProducer(inputStream, StandardCharsets.UTF_8);
    }

    default public void setXmlProducer(InputStream inputStream, Charset charset) {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        InputStreamReader reader = new InputStreamReader(bufferedInputStream, charset);
        InputSource inputSource = new InputSource(reader);
        XmlProducer xmlProducer = new XmlProducer(inputSource);
        setDataSetProducer(new AutocloseDataSetProducer(xmlProducer, inputStream));
    }

    // FlatXmlProducer

    default public void setFlatXmlProducer(String file) throws IOException {
        setFlatXmlProducer(new File(file));
    }

    default public void setFlatXmlProducer(File file) throws IOException {
        setFlatXmlProducer(file, StandardCharsets.UTF_8);
    }

    default public void setFlatXmlProducer(File file, Charset charset) throws IOException {
        FileInputStream inputStream = new FileInputStream(file);
        InputStreamReader characterStream = new InputStreamReader(new BufferedInputStream(inputStream), charset);
        InputSource xmlSource = new InputSource(characterStream);
        FlatXmlProducer dataSetProducer = new FlatXmlProducer(xmlSource);
        AutocloseDataSetProducer closeableDataSetProducer = new AutocloseDataSetProducer(dataSetProducer, inputStream);
        setDataSetProducer(closeableDataSetProducer);
    }

    default public void setFlatXmlProducer(InputStream inputStream) {
        setFlatXmlProducer(inputStream, StandardCharsets.UTF_8);
    }

    default public void setFlatXmlProducer(InputStream inputStream, Charset charset) {
        InputStreamReader characterStream = new InputStreamReader(new BufferedInputStream(inputStream), charset);
        InputSource xmlSource = new InputSource(characterStream);
        FlatXmlProducer dataSetProducer = new FlatXmlProducer(xmlSource);
        AutocloseDataSetProducer autocloseDataSetProducer = new AutocloseDataSetProducer(dataSetProducer, inputStream);
        setDataSetProducer(autocloseDataSetProducer);
    }

    //

    default public void setDataSetProducer(IDataSet dataSet) {
        setDataSetProducer(new DataSetSourceProducerAdapter(dataSet));
    }

    public void setDataSetProducer(IDataSetProducer dataSetProducer);

}
