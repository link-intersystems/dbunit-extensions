package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.producer.support.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.detection.Order;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import com.link_intersystems.dbunit.stream.resource.file.xml.XmlDataSetFile;
import com.link_intersystems.util.config.properties.ConfigProperties;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@Order
public class XmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {

    private ConfigProperties dataSetFileConfig;

    public XmlDataSetDetector(ConfigProperties dataSetFileConfig) {
        this.dataSetFileConfig = dataSetFileConfig;
    }

    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(File file) {
        XmlDataSetFile xmlDataSetFile = new XmlDataSetFile(file);
        xmlDataSetFile.setCharset(dataSetFileConfig.getProperty(DataSetFileConfig.CHARSET));
        return xmlDataSetFile;
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
