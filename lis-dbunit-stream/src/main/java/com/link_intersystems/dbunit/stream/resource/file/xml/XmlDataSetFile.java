package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.AbstractDataSetFile;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.io.FilePath;

import java.io.File;
import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetFile extends AbstractDataSetFile {

    XmlDataSetFile(FilePath filePath) {
        super(filePath);
    }

    @Override
    protected void setProducer(DataSetProducerSupport producerSupport, FilePath filePath) throws IOException {
        producerSupport.setXmlProducer(filePath.toAbsoluteFile());
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, FilePath filePath) throws IOException {
        consumerSupport.setXmlConsumer(filePath.toAbsoluteFile());
    }

}
