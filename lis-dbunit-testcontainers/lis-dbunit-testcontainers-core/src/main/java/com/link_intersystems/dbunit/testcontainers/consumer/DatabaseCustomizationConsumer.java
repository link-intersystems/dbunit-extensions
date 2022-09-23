package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseCustomizationConsumer extends JdbcContainerAwareDataSetConsumer implements ChainableDataSetConsumer {

    @Override
    public void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        try {
            beforeStartDataSet(getJdbcContainer());
            super.startDataSet(jdbcContainer);
            afterStartDataSet();
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    protected void beforeStartDataSet(JdbcContainer jdbcContainer) throws Exception {
    }

    protected void afterStartDataSet() throws Exception {
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            beforeEndDataSet();
            super.endDataSet();
            afterEndDataSet();
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    protected void beforeEndDataSet() throws Exception {
    }

    protected void afterEndDataSet() throws Exception {
    }
}
