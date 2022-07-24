package com.link_intersystems.dbunit.migration.detection.csv;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetFile extends AbstractDataSetFile {

    CsvDataSetFile(File file) {
        super(file);
    }

    @Override
    protected void setProducer(DataSetProducerSupport producerSupport, File file) {
        producerSupport.setCsvProducer(file);
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, File file) {
        consumerSupport.setCsvConsumer(file);
    }

}
