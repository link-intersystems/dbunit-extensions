package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

import static com.link_intersystems.dbunit.test.DBUnitAssertions.STRICT;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class CopyDataSetConsumerTest {

    @Test
    void getDataSet(Connection connection) throws DatabaseUnitException, SQLException {
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(new DatabaseConnection(connection), false);
        FilteredDataSet filteredDataSet = new FilteredDataSet(new String[]{"actor", "film_actor", "film"}, databaseDataSet);
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(filteredDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        dataSetProducerAdapter.setConsumer(copyDataSetConsumer);
        dataSetProducerAdapter.produce();

        STRICT.assertDataSetEquals(filteredDataSet, copyDataSetConsumer.getDataSet());
    }


}