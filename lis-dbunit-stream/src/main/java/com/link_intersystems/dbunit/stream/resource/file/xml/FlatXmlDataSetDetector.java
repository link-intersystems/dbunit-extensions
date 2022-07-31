package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;

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
        flatXmlDataSetFile.setColumnSensing(dataSetFileConfig.isColumnSensing());
        flatXmlDataSetFile.setCharset(dataSetFileConfig.getCharset());
        return flatXmlDataSetFile;
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        producerSupport.setFlatXmlProducer(inputStream, dataSetFileConfig.getCharset());
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) throws DataSetException {
        return !metaData.getTableName().equals("table") &&
                !(metaData.getColumns().length == 1
                        && metaData.getColumns()[0].getColumnName().equals("name")
                );
    }
}
