package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.DataSourceConsumer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.NullDataSourceConsumer;
import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseCustomizationConsumer extends DefaultContainerAwareDataSetConsumer implements ChainableDataSetConsumer {

    private DataSourceConsumer customizeDatabaseOnStartDataSet = NullDataSourceConsumer.INSTANCE;
    private DataSourceConsumer customizeDatabaseOnEndDataSet = NullDataSourceConsumer.INSTANCE;

    /**
     * A {@link DataSourceConsumer} that will be invoked with the test containers {@link DataSource} on {@link #startDataSet()}} .
     *
     * @param customizeDatabaseOnStartDataSet
     */
    public void setCustomizeDatabaseOnStartDataSet(DataSourceConsumer customizeDatabaseOnStartDataSet) {
        this.customizeDatabaseOnStartDataSet = requireNonNull(customizeDatabaseOnStartDataSet);
    }

    /**
     * A {@link DataSourceConsumer} that will be invoked with the test containers {@link DataSource} on {@link #endDataSet()}.
     *
     * @param customizeDatabaseOnEndDataSet
     */
    public void setCustomizeDatabaseOnEndDataSet(DataSourceConsumer customizeDatabaseOnEndDataSet) {
        this.customizeDatabaseOnEndDataSet = requireNonNull(customizeDatabaseOnEndDataSet);
    }

    @Override
    public void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        DataSource dataSource = jdbcContainer.getDataSource();
        try {
            customizeDatabaseOnStartDataSet.consume(dataSource);

            super.startDataSet(jdbcContainer);
        } catch (SQLException e) {
            throw new DataSetException("StartDataSetConsumer threw exception.", e);
        }
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            DataSource dataSource = getJdbcContainer().getDataSource();
            customizeDatabaseOnEndDataSet.consume(dataSource);

            super.endDataSet();
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }
}
