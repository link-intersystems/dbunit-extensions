package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.AbstractDataSetFile;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;

import java.io.File;
import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetFile extends AbstractDataSetFile {

    XmlDataSetFile(File file) {
        super(file);
    }

    @Override
    protected void setProducer(DataSetProducerSupport producerSupport, File file) throws IOException {
        producerSupport.setXmlProducer(file);
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, File file) throws IOException {
        consumerSupport.setXmlConsumer(file);
    }

}
