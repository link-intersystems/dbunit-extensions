package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.jdbc.ColumnMetaDataList;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.test.db.h2.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaH2DatabaseFactory;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseDataSetConsumerTest {

    @Test
    void endDataSet() throws SQLException, DatabaseUnitException, IOException {
        SakilaH2DatabaseFactory sakilaH2DatabaseFactory = new SakilaH2DatabaseFactory("empty");
        H2Database sakila = sakilaH2DatabaseFactory.create("sakila");

        Connection connection = sakila.getConnection();
        ConnectionMetaData connectionMetaData = new ConnectionMetaData(connection);
        ColumnMetaDataList columnMetaData = connectionMetaData.getColumnMetaDataList("film");
        System.out.println(columnMetaData);
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);

        ITableFilterSimple dbTablesFilter = s -> Arrays.asList("actor", "film", "film_actor", "language").contains(s);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false, dbTablesFilter);

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();

        DBUnitAssertions.LENIENT.assertDataSetNotEquals(tinySakilaDataSet, databaseDataSet);

        DatabaseDataSetConsumer databaseDataSetConsumer = new DatabaseDataSetConsumer(databaseConnection);


        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(databaseDataSetConsumer);
        dataSetProducerAdapter.produce();

        IDataSet dataSet = databaseDataSetConsumer.getDataSet();
        DBUnitAssertions.LENIENT.assertDataSetEquals(tinySakilaDataSet, dataSet);

        databaseDataSet = new DatabaseDataSet(databaseConnection, false, dbTablesFilter);
        DBUnitAssertions.LENIENT.assertDataSetEquals(tinySakilaDataSet, databaseDataSet);
    }
}