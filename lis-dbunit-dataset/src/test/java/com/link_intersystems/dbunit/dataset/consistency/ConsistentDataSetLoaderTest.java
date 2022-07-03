package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDataSetLoader;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyTestDBExtension;
import com.link_intersystems.test.ComponentTest;
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
@ExtendWith(SakilaTinyTestDBExtension.class)
@ComponentTest
public class ConsistentDataSetLoaderTest {

    private ConsistentDataSetLoader dataSetLoader;
    private DatabaseConnection databaseConnection;

    @BeforeEach
    void setUp(Connection sakilaConnection) throws DatabaseUnitException {
        databaseConnection = new DatabaseConnection(sakilaConnection);
        dataSetLoader = new ConsistentDataSetLoader(databaseConnection);
    }

    @Test
    void consistentLoad() throws DatabaseUnitException {
        IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", 1);

        String[] tableNames = dataSet.getTableNames();
        assertArrayEquals(new String[]{"film_actor", "film", "actor", "language"}, tableNames);

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
}

