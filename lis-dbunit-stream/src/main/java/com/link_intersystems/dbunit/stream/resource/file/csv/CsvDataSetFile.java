package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.resource.file.AbstractDataSetFile;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.io.FilePath;

import java.io.File;
import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetFile extends AbstractDataSetFile {

    CsvDataSetFile(Path filePath) {
        super(filePath);
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
