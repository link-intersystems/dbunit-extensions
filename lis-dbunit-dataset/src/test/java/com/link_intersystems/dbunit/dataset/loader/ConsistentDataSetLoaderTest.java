package com.link_intersystems.dbunit.dataset.loader;

import com.link_intersystems.dbunit.dataset.BuildProperties;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.test.ComponentTest;
import com.link_intersystems.test.db.sakila.SakilaTestDBExtension;
import com.link_intersystems.test.db.sakila.SakilaTinyTestDBExtension;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

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
        IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(1));

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

    @Test
    void sakilaExport() throws SQLException, DatabaseUnitException, IOException {
        IDataSet dataSet = dataSetLoader.load("SELECT * from actor where actor_id in (1)");

        BuildProperties buildProperties = new BuildProperties();
        File buildOutputDirectory = buildProperties.getBuildOutputDirectory();

        File exportFile = new File(buildOutputDirectory, "sakila-2.xml");
        FlatXmlWriter flatXmlWriter = new FlatXmlWriter(new FileOutputStream(exportFile));
        flatXmlWriter.write(dataSet);
    }
}

