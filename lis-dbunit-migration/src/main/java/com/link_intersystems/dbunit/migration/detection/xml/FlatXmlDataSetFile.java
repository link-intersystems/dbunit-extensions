package com.link_intersystems.dbunit.migration.detection.xml;

import com.link_intersystems.dbunit.migration.detection.csv.AbstractDataSetFile;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;

import java.io.File;
import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetFile extends AbstractDataSetFile {

    FlatXmlDataSetFile(File file) {
        super(file);
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
