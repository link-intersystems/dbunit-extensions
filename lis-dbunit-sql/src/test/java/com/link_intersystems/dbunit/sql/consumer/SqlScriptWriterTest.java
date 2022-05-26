package com.link_intersystems.dbunit.sql.consumer;

import com.link_intersystems.sql.dialect.DefaultSqlDialect;
import com.link_intersystems.sql.dialect.SqlDialect;
import com.link_intersystems.sql.format.SqlFormatSettings;
import com.link_intersystems.test.db.DBSetup;
import com.link_intersystems.test.db.sakila.SakilaSlimDB;
import com.link_intersystems.test.db.sakila.SakilaSlimTestDBExtension;
import com.link_intersystems.test.jdbc.H2Database;
import com.link_intersystems.test.jdbc.SqlScript;
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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.StringReader;
import java.io.StringWriter;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaSlimTestDBExtension.class)
class SqlScriptWriterTest {

    public static final String SELECT_ACTOR = "select * from actor where actor_id = ?";
    private H2Database sakilaDatabase;
    private IDataSet dataSet;

    @BeforeEach
    void setUp(H2Database sakilaDatabase, DBSetup dbSetup) throws DatabaseUnitException, SQLException {
        this.sakilaDatabase = sakilaDatabase;
        Connection connection = sakilaDatabase.getConnection();
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false, H2Database.SYSTEM_TABLE_PREDICATE.negate()::test);
        dataSet = new FilteredDataSet(new DatabaseSequenceFilter(databaseConnection, dbSetup.getTableNames().toArray(new String[0])), databaseDataSet);
    }

    @Test
    void writeSqlStatements() throws DataSetException, SQLException {
        Connection connection = sakilaDatabase.getConnection();
        Map<String, Object> initialActor = getRow(connection, SELECT_ACTOR, 1);
        assertNotNull(initialActor);

        StringWriter writer = new StringWriter();
        SqlDialect sqlDialect = new DefaultSqlDialect();
        SqlScriptWriter sqlStatementWriter = new SqlScriptWriter(sqlDialect, writer);
        SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
        sqlStatementWriter.setSqlFormatSettings(sqlFormatSettings);

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(dataSet);
        dataSetProducerAdapter.setConsumer(sqlStatementWriter);
        dataSetProducerAdapter.produce();


        sakilaDatabase.reset();
        SakilaSlimDB sakilaSlimDB = new SakilaSlimDB();
        sakilaSlimDB.getSchemaScript().execute(connection);
        sakilaSlimDB.getDdlScript().execute(connection);

        Map<String, Object> actorAfterReset = getRow(connection, SELECT_ACTOR, 1);
        assertNull(actorAfterReset);

        SqlScript insertScriptThatWasGenerated = new SqlScript(() -> new StringReader(writer.toString()));
        insertScriptThatWasGenerated.execute(connection);

        Map<String, Object> actorAfterExecuteScript = getRow(connection, SELECT_ACTOR, 1);
        assertEquals(initialActor, actorAfterExecuteScript);
    }

    private Map<String, Object> getRow(Connection connection, String sql, Object... args) throws SQLException {
        LinkedHashMap row = null;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[i]);
            }

            if (ps.execute()) {
                ResultSet resultSet = ps.getResultSet();

                if (resultSet.next()) {
                    row = new LinkedHashMap();

                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        int columnIndex = i + 1;
                        String columnName = metaData.getColumnName(columnIndex);
                        Object columnValue = resultSet.getObject(columnIndex);
                        row.put(columnName, columnValue);
                    }
                }
            }
        }

        return row;
    }
}