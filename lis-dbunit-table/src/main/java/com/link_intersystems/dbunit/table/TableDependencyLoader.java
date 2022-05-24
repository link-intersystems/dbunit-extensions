package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.Dependency;
import com.link_intersystems.dbunit.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.DependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.ExistsSubqueryDependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.JoinDependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
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

    public static enum DependencyDirection {
        OUTGOING {
            @Override
            public List<Dependency> getDependencies(TableDependencyRepository tableDependencyRepository, String tableName) throws DataSetException {
                return tableDependencyRepository.getOutgoingDependencies(tableName);
            }

            @Override
            public Dependency.Edge getSourceEdge(Dependency dependency) {
                return dependency.getSourceEdge();
            }

            @Override
            public Dependency.Edge getTargetEdge(Dependency dependency) {
                return dependency.getTargetEdge();
            }
        }, INCOMING {
            @Override
            public List<Dependency> getDependencies(TableDependencyRepository tableDependencyRepository, String tableName) throws DataSetException {
                return tableDependencyRepository.getIncomingDependencies(tableName);
            }

            @Override
            public Dependency.Edge getSourceEdge(Dependency dependency) {
                return dependency.getTargetEdge();
            }

            @Override
            public Dependency.Edge getTargetEdge(Dependency dependency) {
                return dependency.getSourceEdge();
            }
        };

        public abstract List<Dependency> getDependencies(TableDependencyRepository tableDependencyRepository, String tableName) throws DataSetException;

        public abstract Dependency.Edge getSourceEdge(Dependency dependency);

        public abstract Dependency.Edge getTargetEdge(Dependency dependency);
    }

    private final IDatabaseConnection databaseConnection;

    private final TableDependencyRepository tableDependencyRepository;
    private DependencyStatementFactory dependencyStatementFactory;
    private final TableMetaDataRepository tableMetaDataRepository;

    public TableDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, JoinDependencyStatementFactory.INSTANCE);
    }

    public TableDependencyLoader(IDatabaseConnection databaseConnection, DependencyStatementFactory dependencyStatementFactory) throws DataSetException {
        this.databaseConnection = databaseConnection;

        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);
        this.dependencyStatementFactory = Objects.requireNonNull(dependencyStatementFactory);
    }


    public void loadTables(ITable sourceTable, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        List<Dependency> dependencies = direction.getDependencies(tableDependencyRepository, tableMetaData.getTableName());

        for (Dependency dependency : dependencies) {
            if (loadContext.follow(dependency)) {
                loadDependency(sourceTable, dependency, direction, loadContext);
            }
        }
    }

    private void loadDependency(ITable sourceTable, Dependency dependency, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        DatabaseConfig config = databaseConnection.getConfig();
        Dependency.Edge sourceEdge = direction.getSourceEdge(dependency);
        Dependency.Edge targetEdge = direction.getTargetEdge(dependency);
        SqlStatement sqlStatement = dependencyStatementFactory.create(config, sourceTable, sourceEdge, targetEdge);

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
