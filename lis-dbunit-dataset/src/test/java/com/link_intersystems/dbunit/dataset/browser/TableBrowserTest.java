package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dataset.DataSetAssertions;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.test.ComponentTest;
import com.link_intersystems.test.db.sakila.SakilaTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTestDBExtension.class)
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
        filmActor.with("film_id").eq(200);
        BrowseTable actor = filmActor.browseNatural("actor");
        actor.with("first_name").like("W%");
        BrowseTable film = filmActor.browseNatural("film");
        film.browse("language").on("language_id").references("language_id");

        IDataSet dataSet = TableBrowser.browse(databaseConnection, filmActor);

        String[] tableNames = dataSet.getTableNames();
        assertArrayEquals(new String[]{"film_actor", "actor", "film", "language"}, tableNames);

        ITable filmActorTable = dataSet.getTable("film_actor");
        assertEquals(3, filmActorTable.getRowCount(), "film_actor entity count");
        TableUtil filmActorUtil = new TableUtil(filmActorTable);

        assertNotNull(filmActorUtil.getRowById(9, 200));
        assertNotNull(filmActorUtil.getRowById(102, 200));
        assertNotNull(filmActorUtil.getRowById(139, 200));

        ITable actorTable = dataSet.getTable("actor");
        assertEquals(1, actorTable.getRowCount(), "actor entity count");
        TableUtil actorUtil = new TableUtil(actorTable);
        assertNotNull(actorUtil.getRowById(102));

        ITable filmTable = dataSet.getTable("film");
        assertEquals(1, filmTable.getRowCount(), "film entity count");
        TableUtil filmUtil = new TableUtil(filmTable);
        assertNotNull(filmUtil.getRowById(200));

        ITable languageTable = dataSet.getTable("language");
        assertEquals(1, languageTable.getRowCount(), "language entity count");
        TableUtil languageUtil = new TableUtil(languageTable);
        assertNotNull(languageUtil.getRowById(1));
    }

    @Test
    void completeBrowseTest() throws DatabaseUnitException {
        BrowseTable actor = new BrowseTable("actor");
        actor.with("actor_id").in(1, 2, 3);
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


        IDataSet dataSet = TableBrowser.browse(databaseConnection, actor);
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
        assertEquals(3, actorTable.getRowCount(), "actor entity count");
        TableUtil actorUtil = new TableUtil(actorTable);
        assertNotNull(actorUtil.getRowById(1));
        assertNotNull(actorUtil.getRowById(2));
        assertNotNull(actorUtil.getRowById(3));

        assertions.assertRowCount("film_actor", 66);
        assertions.assertRowCount("film", 66);

        ITable languageTable = dataSet.getTable("language");
        assertEquals(1, languageTable.getRowCount(), "language entity count");
        TableUtil languageUtil = new TableUtil(languageTable);
        assertNotNull(languageUtil.getRowById(1));

        assertions.assertRowCount("inventory", 284);
        assertions.assertRowCount("store", 2);
        assertions.assertRowCount("rental", 1003);
        assertions.assertRowCount("payment", 1003);
        assertions.assertRowCount("customer", 485);
        assertions.assertRowCount("address", 487);
        assertions.assertRowCount("city", 486);
        assertions.assertRowCount("country", 106);
    }

}

