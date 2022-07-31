package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.io.File;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetDetector extends AbstractXmlTableMetaDataDataSetFileDetector {

    @Override
    protected DataSetFile dataSetFileDetectedSucessfully(File file) {
        return new FlatXmlDataSetFile(file);
    }

    @Override
    protected void setProducer(DefaultDataSetProducerSupport producerSupport, InputStream inputStream) {
        producerSupport.setFlatXmlProducer(inputStream);
    }

    @Override
    protected boolean isMatch(ITableMetaData metaData) throws DataSetException {
        return !metaData.getTableName().equals("table") &&
                !(metaData.getColumns().length == 1
                        && metaData.getColumns()[0].getColumnName().equals("name")
                );
    }
}
