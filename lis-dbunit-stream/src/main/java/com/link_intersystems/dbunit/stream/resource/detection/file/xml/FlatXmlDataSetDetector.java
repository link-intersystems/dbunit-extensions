package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.producer.support.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.detection.Order;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFileConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@Order
public class FlatXmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {

    private FlatXmlDataSetFileConfig config;

    public FlatXmlDataSetDetector(FlatXmlDataSetFileConfig config) {
        this.config = config;
    }

    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(File file) {
        FlatXmlDataSetFile flatXmlDataSetFile = new FlatXmlDataSetFile(file);

        flatXmlDataSetFile.setColumnSensing(config.isColumnSensing());
        flatXmlDataSetFile.setCharset(config.getCharset());

        return flatXmlDataSetFile;
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        producerSupport.setFlatXmlProducer(inputStream, config.getCharset());
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) throws DataSetException {
        return !metaData.getTableName().equals("table") &&
                !(metaData.getColumns().length == 1
                        && metaData.getColumns()[0].getColumnName().equals("name")
                );
    }
}
