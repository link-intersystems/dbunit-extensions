package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {
    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(Path path) {
        return new XmlDataSetFile(path);
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        producerSupport.setXmlProducer(inputStream);
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) {
        return true;
    }
}
