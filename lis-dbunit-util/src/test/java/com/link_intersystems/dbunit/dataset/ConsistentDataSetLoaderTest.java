package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.BuildProperties;
import com.link_intersystems.ComponentTest;
import com.link_intersystems.dbunit.table.TableQueries;
import com.link_intersystems.test.db.sakila.SakilaTestDBExtension;
import com.link_intersystems.test.jdbc.H2InMemoryDB;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
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
@ExtendWith(SakilaTestDBExtension.class)
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
        IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(200));

        String[] tableNames = dataSet.getTableNames();
        assertArrayEquals(new String[]{"film_actor", "film", "language", "actor"}, tableNames);

        ITable filmActorTable = dataSet.getTable("film_actor");
        assertEquals(3, filmActorTable.getRowCount(), "film_actor entity count");
        TableQueries filmActorTableQueries = new TableQueries(filmActorTable);

        assertNotNull(filmActorTableQueries.getRowById(9, 200));
        assertNotNull(filmActorTableQueries.getRowById(102, 200));
        assertNotNull(filmActorTableQueries.getRowById(139, 200));

        ITable actorTable = dataSet.getTable("actor");
        assertEquals(3, actorTable.getRowCount(), "actor entity count");
        TableQueries actorTableQueries = new TableQueries(actorTable);
        assertNotNull(actorTableQueries.getRowById(9));
        assertNotNull(actorTableQueries.getRowById(102));
        assertNotNull(actorTableQueries.getRowById(139));

        ITable filmTable = dataSet.getTable("film");
        assertEquals(1, filmTable.getRowCount(), "film entity count");
        TableQueries filmTableQueries = new TableQueries(filmTable);
        assertNotNull(filmTableQueries.getRowById(200));

        ITable languageTable = dataSet.getTable("language");
        assertEquals(1, languageTable.getRowCount(), "language entity count");
        TableQueries languageTableQueries = new TableQueries(languageTable);
        assertNotNull(languageTableQueries.getRowById(1));
    }

    @Test
    void sakilaExport() throws SQLException, DatabaseUnitException, IOException {
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, true, H2InMemoryDB.SYSTEM_TABLE_PREDICATE.negate()::test);

        BuildProperties buildProperties = new BuildProperties();
        File buildOutputDirectory = buildProperties.getBuildOutputDirectory();

        File exportFile = new File(buildOutputDirectory, "sakila.xml");
        FlatXmlWriter flatXmlWriter = new FlatXmlWriter(new FileOutputStream(exportFile));
        flatXmlWriter.write(databaseDataSet);
    }
}

