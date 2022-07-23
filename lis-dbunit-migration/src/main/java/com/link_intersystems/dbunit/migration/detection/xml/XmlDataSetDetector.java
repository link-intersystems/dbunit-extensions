package com.link_intersystems.dbunit.migration.detection.xml;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {
    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(File file) {
        return new XmlDataSetFile(file);
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        producerSupport.setXmlProducer(inputStream);
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) throws DataSetException {
        return true;
    }
}
