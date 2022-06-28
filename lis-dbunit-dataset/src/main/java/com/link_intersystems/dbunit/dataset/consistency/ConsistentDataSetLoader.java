package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.browser.main.TableBrowser;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.table.DatabaseTableReferenceLoader;
import com.link_intersystems.dbunit.table.TableReferenceLoader;
import com.link_intersystems.dbunit.table.TableReferenceTraversal;
import com.link_intersystems.jdbc.ConnectionMetaData;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;

import java.sql.*;

/**
 * Loads a consistent data set based on an SQL query by following all foreign keys from the
 * selected table of the SQL query. E.g.
 *
 * <pre>
 *     IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(200));
 * </pre>
 *
 * <h2>Known issues</h2>
 * All known issues are issues that are planned to be solved in the next releases.
 *
 * <ul>
 *     <li>Cyclic dependencies between entities lead to a StackOverflowError.
 *     If you have two tables A and B and two entities that reference each other, e.g. A1 -> B1 -> A1 ..., you will end up
 *     in a {@link StackOverflowError}.
 *     If you only have a cycle between tables but not between the entities of that tables it will work. E.g.
 *     if you have three tables A, B and C and you have 5 entities A1, B1, C1, B2 and A2 that reference each other in a non
 *     cyclic way, like A1 -> B1 -> C1 -> B2 -> A2, they can be loaded by the {@link ConsistentDataSetLoader}.
 *     </li>
 *     <li>Only full single table may be selected (* or table_alias.*). Multiple tables in the projection will not work.</li>
 * </ul>
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 * @see TableBrowser
 */
public class ConsistentDataSetLoader {

    private final IDatabaseConnection databaseConnection;
    private final TableMetaDataRepository tableMetaDataRepository;

    public ConsistentDataSetLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
    }

    public IDataSet load(String sqlQuery, Object... args) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            return load(connection, sqlQuery, args);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    protected IDataSet load(Connection connection, String sqlQuery, Object[] args) throws DataSetException {
        try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i + 1, args[0]);
            }

            if (ps.execute()) {
                ResultSet resultSet = ps.getResultSet();
                ResultSetMetaData metaData = resultSet.getMetaData();
                String tableName = metaData.getTableName(1);

                ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, resultSet);
                CachedResultSetTable mainTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                DatabaseConnection databaseConnection = new DatabaseConnection(connection);
                return new ConsistentDatabaseDataSet(databaseConnection, mainTable);
            }
        } catch (SQLException | DatabaseUnitException e) {
            throw new DataSetException(e);
        }

        return new DefaultDataSet();
    }

}

