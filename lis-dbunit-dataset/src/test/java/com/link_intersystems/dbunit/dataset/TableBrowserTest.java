package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.browser.TableBrowser;
import com.link_intersystems.dbunit.dsl.BrowseTable;
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

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaTestDBExtension.class)
@ComponentTest
public class TableBrowserTest {

    private DatabaseConnection databaseConnection;

    @BeforeEach
    void setUp(Connection sakilaConnection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(sakilaConnection);
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

}

