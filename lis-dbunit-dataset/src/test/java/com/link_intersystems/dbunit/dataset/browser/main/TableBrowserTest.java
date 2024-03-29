package com.link_intersystems.dbunit.dataset.browser.main;

import com.link_intersystems.dbunit.dataset.DataSetAssertions;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyExtension;
import com.link_intersystems.test.ComponentTest;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaTinyExtension
@ComponentTest
public class TableBrowserTest {

    private DatabaseConnection databaseConnection;
    private TableBrowser tableBrowser;

    @BeforeEach
    void setUp(Connection sakilaConnection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(sakilaConnection);
        tableBrowser = new TableBrowser(databaseConnection);
    }

    @Test
    void browse() throws DatabaseUnitException {
        BrowseTable filmActor = new BrowseTable("film_actor");
        filmActor.with("film_id").eq(1);
        BrowseTable actor = filmActor.browse("actor").natural();
        actor.with("first_name").like("P%");
        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").on("language_id").references("language_id");

        IDataSet dataSet = tableBrowser.browse(filmActor);

        String[] tableNames = dataSet.getTableNames();
        assertArrayEquals(new String[]{"film_actor", "actor", "film", "language"}, tableNames);

        ITable filmActorTable = dataSet.getTable("film_actor");
        assertEquals(1, filmActorTable.getRowCount(), "film_actor entity count");
        TableUtil filmActorUtil = new TableUtil(filmActorTable);

        assertNotNull(filmActorUtil.getRowById(1, 1));

        ITable actorTable = dataSet.getTable("actor");
        assertEquals(1, actorTable.getRowCount(), "actor entity count");
        TableUtil actorUtil = new TableUtil(actorTable);
        assertNotNull(actorUtil.getRowById(1));

        ITable filmTable = dataSet.getTable("film");
        assertEquals(1, filmTable.getRowCount(), "film entity count");
        TableUtil filmUtil = new TableUtil(filmTable);
        assertNotNull(filmUtil.getRowById(1));

        ITable languageTable = dataSet.getTable("language");
        assertEquals(1, languageTable.getRowCount(), "language entity count");
        TableUtil languageUtil = new TableUtil(languageTable);
        assertNotNull(languageUtil.getRowById(1));
    }

    @Test
    void completeBrowseTest() throws DatabaseUnitException {
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


        IDataSet dataSet = tableBrowser.browse(actor);
        DataSetAssertions assertions = new DataSetAssertions(dataSet);

        String[] tableNames = dataSet.getTableNames();
        assertArrayEquals(new String[]{
                "actor",
                "film_actor",
                "film",
                "language",
                "inventory",
                "store",
                "staff",
                "address",
                "city",
                "country",
                "rental",
                "payment",
                "customer",
        }, tableNames);


        ITable actorTable = dataSet.getTable("actor");
        assertEquals(2, actorTable.getRowCount(), "actor entity count");
        TableUtil actorUtil = new TableUtil(actorTable);
        assertNotNull(actorUtil.getRowById(1));
        assertNotNull(actorUtil.getRowById(2));

        assertions.assertRowCount("film_actor", 44);
        assertions.assertRowCount("film", 44);

        ITable languageTable = dataSet.getTable("language");
        assertEquals(1, languageTable.getRowCount(), "language entity count");
        TableUtil languageUtil = new TableUtil(languageTable);
        assertNotNull(languageUtil.getRowById(1));

        assertions.assertRowCount("inventory", 199);
        assertions.assertRowCount("store", 2);
        assertions.assertRowCount("rental", 692);
        assertions.assertRowCount("payment", 692);
        assertions.assertRowCount("customer", 410);
        assertions.assertRowCount("address", 412);
        assertions.assertRowCount("city", 412);
        assertions.assertRowCount("country", 102);
    }

}

