package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.browser.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.TableBrowser;
import com.link_intersystems.dbunit.sql.consumer.SqlScriptWriter;
import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.format.DefaultLiteralFormatRegistry;
import com.link_intersystems.test.db.DBSetup;
import com.link_intersystems.test.db.sakila.SakilaTestDBExtension;
import com.link_intersystems.test.jdbc.H2Database;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTestDBExtension.class)
@Disabled("manual")
class InsertSqlScriptWriterTest {

    private H2Database sakilaDatabase;
    private IDataSet dataSet;

    @BeforeEach
    void setUp(H2Database sakilaDatabase, DBSetup dbSetup) throws DatabaseUnitException, SQLException {
        this.sakilaDatabase = sakilaDatabase;
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
        tableBrowser.browse(actor);

        dataSet = tableBrowser.getDataSet();
    }

    @Test
    void writeSqlScript() throws DataSetException, IOException {
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