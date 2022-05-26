package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dsl.TableBrowseNode;
import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import com.link_intersystems.dbunit.meta.Dependency;
import com.link_intersystems.dbunit.meta.TableDependencyRepository;
import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.JoinDependencyStatementFactory;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.dbunit.table.TableContext;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowseRefLoader {

    private class BrowseColumns {
        private String sourceTableName;
        private String[] sourceColumns;
        private String targetTableName;
        private String[] targetColumns;

        public BrowseColumns(String sourceTableName, String[] sourceColumns, String targetTableName, String[] targetColumns) {
            this.sourceTableName = sourceTableName;
            this.sourceColumns = sourceColumns;
            this.targetTableName = targetTableName;
            this.targetColumns = targetColumns;
        }

        public Dependency.Edge getTargetEdge() throws DataSetException {
            try {
                return toEdge(targetTableName, Arrays.asList(targetColumns));
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Unable to resolve target edge", e);
            }
        }

        public Dependency.Edge getSourceEdge() throws DataSetException {
            try {
                return toEdge(sourceTableName, Arrays.asList(sourceColumns));
            } catch (IllegalStateException e) {
                throw new IllegalStateException("Unable to resolve source edge", e);
            }
        }


        private Dependency.Edge toEdge(String tableName, List<String> columnNames) throws DataSetException {
            ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);

            List<Column> columns = Arrays.stream(tableMetaData.getColumns()).filter(c -> columnNames.contains(c.getColumnName())).collect(Collectors.toList());

            if (columns.size() != columnNames.size()) {
                List<String> tableColumnNames = Arrays.stream(tableMetaData.getColumns()).map(Column::getColumnName).collect(Collectors.toList());
                String msg = MessageFormat.format("Columns {1} are not contained in the table {0} with the columns {2}", tableName, columnNames, tableColumnNames);
                throw new IllegalStateException(msg);
            }

            return new Dependency.Edge(tableMetaData, columns);
        }
    }

    private final IDatabaseConnection databaseConnection;
    private final TableDependencyRepository tableDependencyRepository;
    private final TableMetaDataRepository tableMetaDataRepository;

    public TableBrowseRefLoader(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableDependencyRepository(databaseConnection, tableMetaDataRepository);

    }

    public IDataSet browse(TableBrowseRef tableBrowseRef) throws DataSetException {
        TableContext tableContext = browseTableRef(tableBrowseRef);
        List<ITable> uniqueTables = tableContext.getUniqueTables();
        return new DefaultDataSet(uniqueTables.toArray(new ITable[0]));
    }

    protected TableContext browseTableRef(TableBrowseRef tableBrowseRef) throws DataSetException {
        TableContext tableContext = new TableContext();

        TableBrowseRefSqlStatementBuilder sqlStatementBuilder = new DefaultTableBrowseRefSqlStatementBuilder(tableMetaDataRepository);

        SqlStatement sqlStatement = sqlStatementBuilder.createSqlStatement(tableBrowseRef);
        browse(tableContext, tableBrowseRef, sqlStatement);

        return tableContext;
    }

    private void browseRef(ITable sourceTable, TableBrowseNode targetBrowseNode, TableContext tableContext) throws DataSetException, SQLException {
        TableBrowseRef targetTableRef = targetBrowseNode.getTargetTableRef();

        try {
            BrowseColumns browseColumns = resolveBrowseColumns(sourceTable.getTableMetaData().getTableName(), targetBrowseNode);

            Dependency.Edge sourceEdge = browseColumns.getSourceEdge();
            Dependency.Edge targetEdge = browseColumns.getTargetEdge();

            JoinDependencyStatementFactory statementFactory = new JoinDependencyStatementFactory();
            DatabaseConfig config = databaseConnection.getConfig();

            SqlStatement sqlStatement = statementFactory.create(config, sourceTable, sourceEdge, targetEdge);

            browse(tableContext, targetTableRef, sqlStatement);
        } catch (TableReferenceException e) {
            String msg = MessageFormat.format("Can not browse from source table ''{0}'' to target table ''{1}''", sourceTable.getTableMetaData().getTableName(), targetTableRef.getTableName());
            throw new TableBrowseException(msg, e);
        }
    }

    private void browse(TableContext tableContext, TableBrowseRef targetTableRef, SqlStatement sqlStatement) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            ITable targetTable = sqlStatement.processResultSet(connection, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                String tableName = metaData.getTableName(1);

                ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, rs);
                return new CachedResultSetTable(forwardOnlyResultSetTable);
            });

            tableContext.add(targetTable);

            List<TableBrowseNode> browseNodes = targetTableRef.getBrowseNodes();

            for (TableBrowseNode browseNode : browseNodes) {
                browseRef(targetTable, browseNode, tableContext);
            }

        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private BrowseColumns resolveBrowseColumns(String sourceTableName, TableBrowseNode targetBrowseNode) throws DataSetException {
        String[] sourceColumns = targetBrowseNode.getSourceColumns();
        String[] targetColumns = targetBrowseNode.getTargetColumns();

        TableBrowseRef targetTableRef = targetBrowseNode.getTargetTableRef();
        String targetTableName = targetTableRef.getTableName();

        if (sourceColumns == null && targetColumns == null) {
            List<Dependency> outgoingDependencies = tableDependencyRepository.getOutgoingDependencies(sourceTableName);
            Dependency targetTableDependency = outgoingDependencies.stream().filter(d -> d.getTargetEdge().getTableName().equals(targetTableName)).findFirst().orElse(null);

            if (targetTableDependency == null) {
                List<Dependency> incomingDependencies = tableDependencyRepository.getIncomingDependencies(sourceTableName);
                targetTableDependency = incomingDependencies.stream().filter(d -> d.getSourceEdge().getTableName().equals(sourceTableName)).findFirst().orElse(null);

                if (targetTableDependency == null) {
                    String outgoingReferencesMsg = outgoingDependencies.stream().map(Object::toString).map(s -> "\n\t\t- " + s).collect(Collectors.joining());
                    String incomingReferencesMsg = incomingDependencies.stream().map(Object::toString).map(s -> "\n\t\t- " + s).collect(Collectors.joining());
                    String msg = format("No natural reference found from source table ''{0}'' to target table ''{1}''" +
                                    "\n\t- outgoing references: {2}" +
                                    "\n\t- incoming references: {3}",
                            sourceTableName,
                            targetTableName,
                            outgoingReferencesMsg,
                            incomingReferencesMsg);
                    throw new TableReferenceException(msg);
                }
            }


            Dependency.Edge sourceEdge = targetTableDependency.getSourceEdge();
            sourceColumns = sourceEdge.getColumns().stream().map(Column::getColumnName).toArray(String[]::new);

            Dependency.Edge targetEdge = targetTableDependency.getTargetEdge();
            targetColumns = targetEdge.getColumns().stream().map(Column::getColumnName).toArray(String[]::new);
        }

        return new BrowseColumns(sourceTableName, sourceColumns, targetTableName, targetColumns);
    }


}

