package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.dataset.*;
import com.link_intersystems.dbunit.dsl.TableBrowseNode;
import com.link_intersystems.dbunit.dsl.TableBrowseRef;
import com.link_intersystems.dbunit.meta.TableReference;
import com.link_intersystems.dbunit.meta.TableReferenceException;
import com.link_intersystems.dbunit.meta.TableReferenceRepository;
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
import java.util.List;
import java.util.stream.Collectors;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowser {

    public static IDataSet browse(IDatabaseConnection databaseConnection, TableBrowseRef tableBrowseRef) throws DataSetException {
        TableBrowser tableBrowser = new TableBrowser(databaseConnection);
        tableBrowser.browse(tableBrowseRef);
        return tableBrowser.getDataSet();
    }

    private TableContext tableContext = new TableContext();

    private final IDatabaseConnection databaseConnection;
    private final TableReferenceRepository tableDependencyRepository;
    private final TableMetaDataRepository tableMetaDataRepository;

    public TableBrowser(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        tableDependencyRepository = new TableReferenceRepository(databaseConnection, tableMetaDataRepository);

    }


    public void browse(TableBrowseRef tableBrowseRef) throws DataSetException {
        TableBrowseRefSqlStatementBuilder sqlStatementBuilder = new DefaultTableBrowseRefSqlStatementBuilder(tableMetaDataRepository);

        SqlStatement sqlStatement = sqlStatementBuilder.createSqlStatement(tableBrowseRef);
        browse(tableBrowseRef, sqlStatement);
    }

    private void browse(TableBrowseRef targetTableRef, SqlStatement sqlStatement) throws DataSetException {
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

    private void browseRef(ITable sourceTable, TableBrowseNode targetBrowseNode, TableContext tableContext) throws DataSetException, SQLException {
        TableBrowseRef targetTableRef = targetBrowseNode.getTargetTableRef();

        try {
            BrowseColumns browseColumns = resolveBrowseColumns(sourceTable.getTableMetaData().getTableName(), targetBrowseNode);

            TableReference.Edge sourceEdge = browseColumns.getSourceEdge();
            TableReference.Edge targetEdge = browseColumns.getTargetEdge();

            JoinDependencyStatementFactory statementFactory = new JoinDependencyStatementFactory();
            DatabaseConfig config = databaseConnection.getConfig();

            SqlStatement sqlStatement = statementFactory.create(config, sourceTable, sourceEdge, targetEdge);

            browse(targetTableRef, sqlStatement);
        } catch (TableReferenceException e) {
            String msg = MessageFormat.format("Can not browse from source table ''{0}'' to target table ''{1}''", sourceTable.getTableMetaData().getTableName(), targetTableRef.getTableName());
            throw new TableBrowseException(msg, e);
        }
    }


    private BrowseColumns resolveBrowseColumns(String sourceTableName, TableBrowseNode targetBrowseNode) throws DataSetException {
        String[] sourceColumns = targetBrowseNode.getSourceColumns();
        String[] targetColumns = targetBrowseNode.getTargetColumns();

        TableBrowseRef targetTableRef = targetBrowseNode.getTargetTableRef();
        String targetTableName = targetTableRef.getTableName();

        if (sourceColumns == null && targetColumns == null) {
            List<TableReference> outgoingDependencies = tableDependencyRepository.getOutgoingReferences(sourceTableName);
            TableReference targetTableDependency = outgoingDependencies.stream().filter(d -> d.getTargetEdge().getTableName().equals(targetTableName)).findFirst().orElse(null);

            if (targetTableDependency == null) {
                List<TableReference> incomingDependencies = tableDependencyRepository.getIncomingReferences(sourceTableName);
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


            TableReference.Edge sourceEdge = targetTableDependency.getSourceEdge();
            sourceColumns = sourceEdge.getColumns().stream().map(Column::getColumnName).toArray(String[]::new);

            TableReference.Edge targetEdge = targetTableDependency.getTargetEdge();
            targetColumns = targetEdge.getColumns().stream().map(Column::getColumnName).toArray(String[]::new);
        }

        return new BrowseColumns(tableMetaDataRepository, sourceTableName, sourceColumns, targetTableName, targetColumns);
    }


    public IDataSet getDataSet() {
        return new MergedTablesDataSet(tableContext);
    }

}
