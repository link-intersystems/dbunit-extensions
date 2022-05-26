package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.meta.TableReference;
import com.link_intersystems.dbunit.meta.TableReferenceEdge;
import com.link_intersystems.dbunit.meta.TableReferenceRepository;
import com.link_intersystems.dbunit.sql.statement.DependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.JoinDependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.jdbc.ConnectionMetaData;
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
            public List<TableReference> getDependencies(TableReferenceRepository tableDependencyRepository, String tableName) throws SQLException {
                return tableDependencyRepository.getOutgoingReferences(tableName);
            }

            @Override
            public TableReferenceEdge getSourceEdge(TableReference dependency) {
                return dependency.getSourceEdge();
            }

            @Override
            public TableReferenceEdge getTargetEdge(TableReference dependency) {
                return dependency.getTargetEdge();
            }
        }, INCOMING {
            @Override
            public List<TableReference> getDependencies(TableReferenceRepository tableDependencyRepository, String tableName) throws SQLException {
                return tableDependencyRepository.getIncomingReferences(tableName);
            }

            @Override
            public TableReferenceEdge getSourceEdge(TableReference dependency) {
                return dependency.getTargetEdge();
            }

            @Override
            public TableReferenceEdge getTargetEdge(TableReference dependency) {
                return dependency.getSourceEdge();
            }
        };

        public abstract List<TableReference> getDependencies(TableReferenceRepository tableDependencyRepository, String tableName) throws DataSetException, SQLException;

        public abstract TableReferenceEdge getSourceEdge(TableReference dependency);

        public abstract TableReferenceEdge getTargetEdge(TableReference dependency);
    }

    private final IDatabaseConnection databaseConnection;

    private final TableReferenceRepository tableDependencyRepository;
    private DependencyStatementFactory dependencyStatementFactory;
    private final TableMetaDataRepository tableMetaDataRepository;

    public TableDependencyLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this(databaseConnection, JoinDependencyStatementFactory.INSTANCE);
    }

    public TableDependencyLoader(IDatabaseConnection databaseConnection, DependencyStatementFactory dependencyStatementFactory) throws DataSetException {
        this.databaseConnection = databaseConnection;

        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        try {
            ConnectionMetaData connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection());
            tableDependencyRepository = new TableReferenceRepository(connectionMetaData);
            this.dependencyStatementFactory = Objects.requireNonNull(dependencyStatementFactory);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }


    public void loadTables(ITable sourceTable, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        ITableMetaData tableMetaData = sourceTable.getTableMetaData();

        try {
            List<TableReference> dependencies = direction.getDependencies(tableDependencyRepository, tableMetaData.getTableName());


            for (TableReference dependency : dependencies) {
                if (loadContext.follow(dependency)) {
                    loadDependency(sourceTable, dependency, direction, loadContext);
                }
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private void loadDependency(ITable sourceTable, TableReference dependency, DependencyDirection direction, TableContext loadContext) throws DataSetException {
        TableReferenceEdge sourceEdge = direction.getSourceEdge(dependency);
        TableReferenceEdge targetEdge = direction.getTargetEdge(dependency);

        try {
            SqlStatement sqlStatement = dependencyStatementFactory.create(sourceTable, sourceEdge, targetEdge);
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
