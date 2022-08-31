package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFileConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.Charset;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {

    private DataSetFileConfig dataSetFileConfig;

    public FlatXmlDataSetDetector(DataSetFileConfig dataSetFileConfig) {
        this.dataSetFileConfig = dataSetFileConfig;
    }

    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(File file) {
        FlatXmlDataSetFile flatXmlDataSetFile = new FlatXmlDataSetFile(file);

        Boolean columnSensing = dataSetFileConfig.getProperty(FlatXmlDataSetFileConfig.COLUMN_SENSING_PROPERTY);
        flatXmlDataSetFile.setColumnSensing(columnSensing);

        Charset charset = dataSetFileConfig.getProperty(DataSetFileConfig.CHARSET_PROPERTY);
        flatXmlDataSetFile.setCharset(charset);

        return flatXmlDataSetFile;
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        Charset charset = dataSetFileConfig.getProperty(DataSetFileConfig.CHARSET_PROPERTY);
        producerSupport.setFlatXmlProducer(inputStream, charset);
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) throws DataSetException {
        return !metaData.getTableName().equals("table") &&
                !(metaData.getColumns().length == 1
                        && metaData.getColumns()[0].getColumnName().equals("name")
                );
    }
}
