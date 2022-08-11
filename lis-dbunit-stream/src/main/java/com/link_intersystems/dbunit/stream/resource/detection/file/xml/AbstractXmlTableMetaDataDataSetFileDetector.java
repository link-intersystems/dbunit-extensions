package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractXmlTableMetaDataDataSetFileDetector extends AbstractXmlDataSetFileDetector {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    private DataSetFileConfig detectionConfig;

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
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("DataSet type can not be detected.", e);
                    }
                }

                if (consumerDetector.seemsToBeAFlatXmlDataSet) {
                    return dataSetFileDetectedSucessfully(file);
                }
            }
        } catch (IOException | DataSetException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("DataSet type can not be detected.", e);
            }
        }
        return null;
    }

    protected abstract DataSetFile dataSetFileDetectedSucessfully(File file);

    protected abstract void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream);

    protected abstract boolean isMatch(ITableMetaData metaData) throws DataSetException;
}
