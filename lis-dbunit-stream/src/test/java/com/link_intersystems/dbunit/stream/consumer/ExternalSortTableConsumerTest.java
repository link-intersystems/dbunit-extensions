package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.jdbc.test.db.h2.H2Extension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(H2Extension.class)
@SakilaSlimExtension
class ExternalSortTableConsumerTest {

    private DatabaseDataSet databaseDataSet;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException, SQLException {
        databaseDataSet = new DatabaseDataSet(new DatabaseConnection(connection), false, t -> SakilaSlimDB.getTableNames().contains(t));
    }

    @Test
    void metaDataMaintained() throws DataSetException {
        DefaultTableOrder defaultTableOrder = new DefaultTableOrder();
        ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(defaultTableOrder);
        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        externalSortTableConsumer.setSubsequentConsumer(copyDataSetConsumer);

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(databaseDataSet);
        dataSetProducerAdapter.setConsumer(externalSortTableConsumer);
        dataSetProducerAdapter.produce();

        IDataSet resultDataSet = copyDataSetConsumer.getDataSet();
        DBUnitAssertions.STRICT.assertMetaDataEquals(databaseDataSet.getTableMetaData("actor"), resultDataSet.getTableMetaData("actor"));
    }
}