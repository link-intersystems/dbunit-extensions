package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractXmlTableMetaDataDataSetFileDetector extends AbstractXmlDataSetFileDetector {

    private Logger logger = LoggerFactory.getLogger(AbstractXmlTableMetaDataDataSetFileDetector.class);

    @Override
    protected DataSetFile detectXmlFile(Path path) {
        DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
        try {
            try (FileInputStream fileInputStream = new FileInputStream(path.toFile())) {
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
                    return dataSetFileDetectedSucessfully(path);
                }
            }
        } catch (IOException | DataSetException e) {
        }
        return null;
    }

    protected abstract DataSetFile dataSetFileDetectedSucessfully(Path path);

    protected abstract void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream);

    protected abstract boolean isMatch(ITableMetaData metaData) throws DataSetException;
}
