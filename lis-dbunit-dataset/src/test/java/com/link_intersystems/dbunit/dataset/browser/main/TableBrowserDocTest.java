package com.link_intersystems.dbunit.dataset.browser.main;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaTinyExtension
public class TableBrowserDocTest {

    @Test
    void browse(Connection sakilaConnection) throws DatabaseUnitException {
        BrowseTable filmActor = new BrowseTable("film_actor");
        filmActor.with("film_id").eq(1);

        BrowseTable actor = filmActor.browse("actor").natural();
        actor.with("first_name").like("P%");

        BrowseTable film = filmActor.browse("film").natural();
        film.browse("language").on("language_id").references("language_id");

        TableBrowser tableBrowser = new TableBrowser(new DatabaseConnection(sakilaConnection));
        IDataSet dataSet = tableBrowser.browse(filmActor);
    }

}

