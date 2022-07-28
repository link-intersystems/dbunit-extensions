package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.AbstractDataSetFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetFile extends AbstractDataSetFile {

    FlatXmlDataSetFile(Path path) {
        super(path);
    }

    @Override
    protected void setProducer(DataSetProducerSupport producerSupport, File file) throws IOException {
        producerSupport.setFlatXmlProducer(file);
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, File file) throws IOException {
        consumerSupport.setFlatXmlConsumer(file);
    }

}
