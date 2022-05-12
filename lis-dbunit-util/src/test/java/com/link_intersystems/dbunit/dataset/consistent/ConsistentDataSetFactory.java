package com.link_intersystems.dbunit.dataset.consistent;

import com.link_intersystems.dbunit.repository.meta.Dependency;
import com.link_intersystems.dbunit.repository.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.repository.meta.TableMetaDataRepository;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDataSetFactory {

    private final IDatabaseConnection databaseConnection;
    private final TableDependencyRepository tableDependencyRepository;
    private final TableMetaDataRepository tableMetaDataRepository;


    public ConsistentDataSetFactory(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);
    }

    public IDataSet createDataSet(String sqlQuery, Object... args) throws DataSetException {
        List<ITable> dataSetTables = new ArrayList<>();

        try (Connection connection = databaseConnection.getConnection()) {


            try (PreparedStatement ps = connection.prepareStatement(sqlQuery)) {
                for (int i = 0; i < args.length; i++) {
                    ps.setObject(i + 1, args[0]);
                }


                if (ps.execute()) {
                    ResultSet resultSet = ps.getResultSet();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    String tableName = metaData.getTableName(1);

                    ResultSetTableMetaData2 tableMetaData = new ResultSetTableMetaData2(tableName, resultSet, databaseConnection);
                    ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, resultSet);
                    CachedResultSetTable mainTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                    dataSetTables.add(0, mainTable);
                    dataSetTables.addAll(0, getOutgoingTables(connection, mainTable));
                }
            }
        } catch (SQLException e) {
            throw new DataSetException(e);
        }

        return new AbstractDataSet() {
            @Override
            protected ITableIterator createIterator(boolean b) throws DataSetException {
                return new DefaultTableIterator(dataSetTables.toArray(new ITable[0]));
            }
        };
    }

    private List<ITable> getOutgoingTables(Connection connection, ITable sourceTable) throws DataSetException {
        List<ITable> outgoingTables = new ArrayList<>();

        ITableMetaData tableMetaData = sourceTable.getTableMetaData();
        List<Dependency> outgoingDependencies = tableDependencyRepository.getOutgoingDependencies(tableMetaData.getTableName());

        for (Dependency outgoingDependency : outgoingDependencies) {
            Dependency.Edge sourceEdge = outgoingDependency.getSourceEdge();
            Dependency.Edge targetEdge = outgoingDependency.getTargetEdge();

            try (PreparedStatement ps = prepareStatement(connection, sourceTable, sourceEdge, targetEdge)) {
                if (ps.execute()) {
                    ResultSet resultSet = ps.getResultSet();
                    ResultSetTableMetaData2 targetTableMetaData = new ResultSetTableMetaData2(targetEdge.getTableMetaData().getTableName(), resultSet, databaseConnection);
                    ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(targetTableMetaData, resultSet);
                    CachedResultSetTable targetTable = new CachedResultSetTable(forwardOnlyResultSetTable);
                    outgoingTables.add(0, targetTable);
                    outgoingTables.addAll(0, getOutgoingTables(connection, targetTable));
                }
            } catch (SQLException e) {
                throw new DataSetException(e);
            }

        }

        return outgoingTables;
    }

    private PreparedStatement prepareStatement(Connection connection, ITable sourceTable, Dependency.Edge sourceEdge, Dependency.Edge targetEdge) throws SQLException, DataSetException {
        StringBuilder stmtBuilder = new StringBuilder();
        stmtBuilder.append("SELECT distinct ");

        ITableMetaData targetTableMetaData = targetEdge.getTableMetaData();

        stmtBuilder.append(targetTableMetaData.getTableName());
        stmtBuilder.append(".*");

        stmtBuilder.append(" FROM ");
        String targetTableName = targetTableMetaData.getTableName();
        stmtBuilder.append(targetTableName);

        stmtBuilder.append(" JOIN ");

        String sourceTableName = sourceTable.getTableMetaData().getTableName();
        stmtBuilder.append(toTableName(sourceTable));

        stmtBuilder.append(" ON ");
        List<Column> sourceColumns = sourceEdge.getColumns();
        List<Column> targetColumns = targetEdge.getColumns();

        for (int i = 0; i < sourceColumns.size(); i++) {
            Column sourceColumn = sourceColumns.get(i);
            Column targetColumn = targetColumns.get(i);

            stmtBuilder.append(targetTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(targetColumn.getColumnName());

            stmtBuilder.append(" = ");

            stmtBuilder.append(sourceTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(sourceColumn.getColumnName());
            if (i < sourceColumns.size() - 1) {
                stmtBuilder.append(" AND ");
            }
        }

        stmtBuilder.append(" WHERE (");

        for (int i = 0; i < sourceColumns.size(); i++) {
            Column sourceColumn = sourceColumns.get(i);
            stmtBuilder.append(sourceTableName);
            stmtBuilder.append(".");
            stmtBuilder.append(sourceColumn.getColumnName());
            if (i < sourceColumns.size() - 1) {
                stmtBuilder.append(", ");
            }
        }

        stmtBuilder.append(") IN (");

        String wherePart = String.join(", ", Collections.nCopies(sourceColumns.size(), "?"));

        int rowCount = sourceTable.getRowCount();
        for (int i = 0; i < rowCount; i++) {
            stmtBuilder.append(" ( ");
            stmtBuilder.append(wherePart);
            stmtBuilder.append(" ) ");
            if (i < rowCount - 1) {
                stmtBuilder.append(", ");
            }
        }

        stmtBuilder.append(")");

        PreparedStatement ps = connection.prepareStatement(stmtBuilder.toString());

        int paramIndex = 1;

        for (int i = 0; i < rowCount; i++) {
            for (int j = 0; j < sourceColumns.size(); j++) {
                Column column = sourceColumns.get(j);
                Object columnValue = sourceTable.getValue(i, column.getColumnName());
                ps.setObject(paramIndex++, columnValue);
            }
        }

        return ps;
    }

    private int[] getColumnIndexes(ITableMetaData tableMetaData, List<Column> columns) throws DataSetException {
        int[] indexes = new int[columns.size()];
        List<Column> tableColumns = Arrays.asList(tableMetaData.getColumns());

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            indexes[i] = tableColumns.indexOf(column);
        }

        return indexes;
    }

    private String toTableName(ITable table) {
        return table.getTableMetaData().getTableName();
    }
}

