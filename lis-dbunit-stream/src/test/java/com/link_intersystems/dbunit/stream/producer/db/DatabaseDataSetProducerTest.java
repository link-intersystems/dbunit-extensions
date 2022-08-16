package com.link_intersystems.dbunit.stream.producer.db;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTinyTestDBExtension.class)
class DatabaseDataSetProducerTest {

    @Test
    void produce(Connection connection) throws DatabaseUnitException, SQLException {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSetProducer databaseDataSetProducer = new DatabaseDataSetProducer(databaseConnection);
        DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
        config.setSchema("sakila");
        ITableFilterSimple tableFilter = t -> SakilaTinyDB.getTableNames().contains(t);
        config.setTableFilter(tableFilter);
        databaseDataSetProducer.setDatabaseDataSetProducerConfig(config);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        databaseDataSetProducer.setConsumer(copyDataSetConsumer);

        databaseDataSetProducer.produce();

        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false, tableFilter);

        DBUnitAssertions.STRICT.assertDataSetEquals(databaseDataSet, copyDataSetConsumer.getDataSet());
    }
}