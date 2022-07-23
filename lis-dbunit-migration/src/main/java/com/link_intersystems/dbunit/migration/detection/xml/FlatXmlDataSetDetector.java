package com.link_intersystems.dbunit.migration.detection.xml;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.migration.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetDetector implements DataSetFileDetector {

    @Override
    public DataSetFile detect(File file) {
        if (file.isDirectory()) {
            return null;
        }

        if (file.getName().endsWith(".xml")) {
            DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
            try {
                try (FileInputStream fileInputStream = new FileInputStream(file)) {
                    producerSupport.setFlatXmlProducer(fileInputStream);
                    IDataSetProducer dataSetProducer = producerSupport.getDataSetProducer();

                    class ConsumerDetector extends DefaultConsumer {

                        private boolean seemsToBeAFlatXmlDataSet;

                        @Override
                        public void startTable(ITableMetaData metaData) throws DataSetException {
                            seemsToBeAFlatXmlDataSet = !metaData.getTableName().equals("table") &&
                                    !(metaData.getColumns().length == 1
                                            && metaData.getColumns()[0].getColumnName().equals("name")
                                    );
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
                        return new FlatXmlDataSetFile(file);
                    }
                }
            } catch (IOException | DataSetException e) {
            }
        }

        return null;
    }
}
