package com.link_intersystems.dbunit.stream.consumer.sql;

import com.link_intersystems.jdbc.MapRowMapper;
import com.link_intersystems.jdbc.test.db.h2.H2Database;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimExtension;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.sql.io.SqlScript;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.*;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaSlimExtension
class SqlScriptWriterTest {

    public static final String SELECT_ACTOR = "select * from actor where actor_id = ?";
    private H2Database sakilaDatabase;
    private IDataSet dataSet;

    @BeforeEach
    void setUp(H2Database sakilaDatabase) throws DatabaseUnitException, SQLException {
        this.sakilaDatabase = sakilaDatabase;
        Connection connection = sakilaDatabase.getConnection();
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false, H2Database.SYSTEM_TABLE_PREDICATE.negate()::test);
        String[] sakilaSlimDBTables = {"actor", "film_actor", "film", "language", "film_category", "category"};
        DatabaseSequenceFilter sequenceFilter = new DatabaseSequenceFilter(databaseConnection, sakilaSlimDBTables);
        dataSet = new FilteredDataSet(sequenceFilter, databaseDataSet);
    }

    @Test
    void writeSqlStatements() throws DataSetException, SQLException {
        Connection connection = sakilaDatabase.getConnection();
        Map<String, Object> initialActor = getRow(connection, SELECT_ACTOR, 1);
        assertNotNull(initialActor);

        StringWriter writer = new StringWriter();
        SqlScriptDataSetConsumer sqlStatementWriter = new SqlScriptDataSetConsumer(writer);
        sqlStatementWriter.setSchema("sakila2");
        SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
        sqlStatementWriter.setSqlFormatSettings(sqlFormatSettings);

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(dataSet);
        dataSetProducerAdapter.setConsumer(sqlStatementWriter);
        dataSetProducerAdapter.produce();


        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA sakila2");
        }

        sakilaDatabase.setSchema("sakila2");

        SakilaSlimDB sakilaSlimDB = new SakilaSlimDB();
        sakilaSlimDB.getDdlScript().execute(connection);

        Map<String, Object> actorAfterReset = getRow(connection, SELECT_ACTOR, 1);
        assertNull(actorAfterReset);

        SqlScript insertScriptThatWasGenerated = new SqlScript(() -> new StringReader(writer.toString()));
        insertScriptThatWasGenerated.execute(connection);

        Map<String, Object> actorAfterExecuteScript = getRow(connection, SELECT_ACTOR, 1);
        assertEquals(initialActor, actorAfterExecuteScript);
    }

    private Map<String, Object> getRow(Connection connection, String sql, Object... args) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }


            if (ps.execute()) {
                ResultSet resultSet = ps.getResultSet();

                if (resultSet.next()) {
                    MapRowMapper mapRowMapper = new MapRowMapper();
                    return mapRowMapper.map(resultSet);
                }
            }
        }

        return null;
    }
}