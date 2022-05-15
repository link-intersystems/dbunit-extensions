package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.Dependency;
import com.link_intersystems.dbunit.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.statement.DependencyStatementFactory;
import com.link_intersystems.dbunit.statement.ExistsSubqueryDependencyStatementFactory;
import com.link_intersystems.dbunit.statement.SqlStatement;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableDependencyLoader {

    private final IDatabaseConnection databaseConnection;

    private final TableDependencyRepository tableDependencyRepository;
    private DependencyStatementFactory dependencyStatementFactory;
    private final TableMetaDataRepository tableMetaDataRepository;

    public TableDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, ExistsSubqueryDependencyStatementFactory.INSTANCE);
    }

    public TableDependencyLoader(IDatabaseConnection databaseConnection, DependencyStatementFactory dependencyStatementFactory) throws DataSetException {
        this.databaseConnection = databaseConnection;

        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);
        this.dependencyStatementFactory = Objects.requireNonNull(dependencyStatementFactory);
    }

    public void loadOutgoingTables(ITable sourceTable, TableContext loadContext) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        List<Dependency> outgoingDependencies = tableDependencyRepository.getOutgoingDependencies(tableMetaData.getTableName());

        for (Dependency outgoingDependency : outgoingDependencies) {
            loadOutgoingDependency(sourceTable, outgoingDependency, loadContext);
        }
    }

    private void loadOutgoingDependency(ITable sourceTable, Dependency outgoingDependency, TableContext loadContext) throws DataSetException {
        DatabaseConfig config = databaseConnection.getConfig();
        SqlStatement sqlStatement = dependencyStatementFactory.create(config, sourceTable, outgoingDependency);

        try {
            Connection connection = databaseConnection.getConnection();
            sqlStatement.executeQuery(connection, ps -> {
                loadTableFromResultSet(ps.executeQuery(), loadContext);
            });
        } catch (DataSetException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    private ITable loadTableFromResultSet(ResultSet resultSet, TableContext loadContext) throws DataSetException, SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        String tableName = metaData.getTableName(1);
        ITableMetaData targetTableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
        ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
        ITable targetTable = new CachedResultSetTable(forwardOnlyResultSetTable);
        loadContext.add(targetTable);
        return targetTable;
    }
}
