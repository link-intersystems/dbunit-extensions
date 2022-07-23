package com.link_intersystems.dbunit.migration.detection.xml;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractXmlTableMetaDataDataSetFileDetector extends AbstractXmlDataSetFileDetector {
    @Override
    protected DataSetFile detectXmlFile(File file) {
        DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
        try {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                setProducer(producerSupport, fileInputStream);
                IDataSetProducer dataSetProducer = producerSupport.getDataSetProducer();

                class ConsumerDetector extends DefaultConsumer {

                    private boolean seemsToBeAFlatXmlDataSet;

                    @Override
                    public void startTable(ITableMetaData metaData) throws DataSetException {
                        seemsToBeAFlatXmlDataSet = isMatch(metaData);
                        throw new DataSetException();
                    }
                }

                ConsumerDetector consumerDetector = new ConsumerDetector();
                dataSetProducer.setConsumer(consumerDetector);

                try {
                    dataSetProducer.produce();
                } catch (DataSetException e) {
                }

                if (consumerDetector.seemsToBeAFlatXmlDataSet) {
                    return dataSetFileDetectedSucessfully(file);
                }
            }
        } catch (IOException | DataSetException e) {
        }
        return null;
    }

    protected abstract DataSetFile dataSetFileDetectedSucessfully(File file);

    protected abstract void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream);

    protected abstract boolean isMatch(ITableMetaData metaData) throws DataSetException;
}
