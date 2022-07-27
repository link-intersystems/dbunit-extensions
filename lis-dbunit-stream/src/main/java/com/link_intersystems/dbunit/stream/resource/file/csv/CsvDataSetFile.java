package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.resource.file.AbstractDataSetFile;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import com.link_intersystems.io.FilePath;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetFile extends AbstractDataSetFile {

    CsvDataSetFile(FilePath filePath) {
        super(filePath);
    }

    @Override
    protected void setProducer(DataSetProducerSupport producerSupport, FilePath filePath) {
        producerSupport.setCsvProducer(filePath.toAbsoluteFile());
    }

    @Override
    protected void setConsumer(DataSetConsumerSupport consumerSupport, FilePath filePath) {
        consumerSupport.setCsvConsumer(filePath.toAbsoluteFile());
    }

}
