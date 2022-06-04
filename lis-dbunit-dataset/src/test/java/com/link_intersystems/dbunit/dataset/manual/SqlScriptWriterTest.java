package com.link_intersystems.dbunit.dataset.manual;

import com.link_intersystems.dbunit.dataset.BuildProperties;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.main.TableBrowser;
import com.link_intersystems.dbunit.sql.consumer.SqlScriptWriter;
import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.format.DefaultLiteralFormatRegistry;
import com.link_intersystems.test.db.H2DatabaseFactory;
import com.link_intersystems.test.db.sakila.SakilaEmptyDB;
import com.link_intersystems.test.db.setup.DBSetupH2DatabaseFactory;
import com.link_intersystems.test.jdbc.H2Database;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

class SqlScriptWriterTest {

    public static void main(String[] args) throws SQLException, DatabaseUnitException, IOException {
        SakilaEmptyDB sakilaEmptyDB = new SakilaEmptyDB();
        H2DatabaseFactory h2DatabaseFactory = new DBSetupH2DatabaseFactory(sakilaEmptyDB);
        H2Database sakilaDatabase = h2DatabaseFactory.create();

        SqlScriptWriterTest insertSqlScriptWriterTest = new SqlScriptWriterTest();
        IDataSet dataSet = insertSqlScriptWriterTest.getDataSet(sakilaDatabase);
        insertSqlScriptWriterTest.writeSqlScript(dataSet);

    }

    IDataSet getDataSet(H2Database sakilaDatabase) throws DatabaseUnitException, SQLException {
        Connection connection = sakilaDatabase.getConnection();
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);

        BrowseTable actor = new BrowseTable("actor");
        actor.with("actor_id").in(1, 2);
        BrowseTable filmActor = actor.browse("film_actor").natural();
        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").natural();
        BrowseTable inventory = film.browse("inventory").natural();
        BrowseTable store = inventory.browse("store").natural();
        BrowseTable staff = store.browse("staff").natural();
        BrowseTable rental = inventory.browse("rental").natural();
        BrowseTable payment = rental.browse("payment").natural();
        BrowseTable customer = payment.browse("customer").natural();

        BrowseTable staffAddress = staff.browse("address")
                .on("address_id")
                .references("address_id");

        BrowseTable staffCity = staffAddress.browse("city").natural();
        staffCity.browse("country").natural();
        BrowseTable address = customer.browse("address").natural();
        BrowseTable city = address.browse("city").natural();
        city.browse("country").natural();

        TableBrowser tableBrowser = new TableBrowser(databaseConnection);
        return tableBrowser.browse(actor);
    }

    void writeSqlScript(IDataSet dataSet) throws DataSetException, IOException {
        BuildProperties buildProperties = new BuildProperties();
        File buildOutputDirectory = buildProperties.getBuildOutputDirectory();
        DefaultSqlDialect sqlDialect = new DefaultSqlDialect();
        DefaultLiteralFormatRegistry literalFormatRegistry = new DefaultLiteralFormatRegistry();
        literalFormatRegistry.put(Types.BLOB, o -> o == null ? "null" : "HEXTORAW('" + bytesToHex((byte[]) o) + "')");
        sqlDialect.setLiteralFormatRegistry(literalFormatRegistry);
        SqlScriptWriter sqlStatementWriter = new SqlScriptWriter(sqlDialect, new PrintWriter(new FileWriter(new File(buildOutputDirectory, "sakila-tiny-db.sql"))));
        sqlStatementWriter.setSchema("sakila");
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(dataSet);
        dataSetProducerAdapter.setConsumer(sqlStatementWriter);
        dataSetProducerAdapter.produce();


    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

}