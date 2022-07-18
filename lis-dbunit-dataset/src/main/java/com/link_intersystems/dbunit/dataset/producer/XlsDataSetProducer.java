package com.link_intersystems.dbunit.dataset.producer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetProducer implements IDataSetProducer {

    private final DataSetProducerAdapter xlsProducerAdapter;

    public XlsDataSetProducer(InputStream inputStream) throws IOException {
        try {
            XlsDataSet xlsDataSet = new XlsDataSet(inputStream);
            xlsProducerAdapter = new DataSetProducerAdapter(xlsDataSet);
        } catch (DataSetException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setConsumer(IDataSetConsumer iDataSetConsumer) throws DataSetException {
        xlsProducerAdapter.setConsumer(iDataSetConsumer);
    }

    @Override
    public void produce() throws DataSetException {
        xlsProducerAdapter.produce();
    }
}
