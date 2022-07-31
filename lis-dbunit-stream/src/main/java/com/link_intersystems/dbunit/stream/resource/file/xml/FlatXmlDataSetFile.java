package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.resource.file.AbstractTextDataSetFile;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetFile extends AbstractTextDataSetFile {

    private boolean columnSensing;

    FlatXmlDataSetFile(File file) {
        super(file);
    }

    @Override
    protected IDataSetProducer createProducer(Reader reader) {
        InputSource xmlSource = new InputSource(reader);
        FlatXmlProducer dataSetProducer = new FlatXmlProducer(xmlSource);
        dataSetProducer.setColumnSensing(columnSensing);
        return dataSetProducer;
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, File file) throws IOException {
        consumerSupport.setFlatXmlConsumer(file);
    }

    public void setColumnSensing(boolean columnSensing) {
        this.columnSensing = columnSensing;
    }
}
