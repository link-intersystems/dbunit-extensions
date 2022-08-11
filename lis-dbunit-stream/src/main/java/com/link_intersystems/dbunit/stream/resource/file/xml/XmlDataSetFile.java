package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.resource.file.AbstractTextDataSetFile;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.xml.XmlProducer;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.IOException;
import java.io.Reader;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetFile extends AbstractTextDataSetFile {

    public XmlDataSetFile(File file) {
        super(file);
    }

    @Override
    protected IDataSetProducer createProducer(Reader reader) {
        InputSource inputSource = new InputSource(reader);
        return new XmlProducer(inputSource);
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, File file) throws IOException {
        consumerSupport.setXmlConsumer(file);
    }

}
