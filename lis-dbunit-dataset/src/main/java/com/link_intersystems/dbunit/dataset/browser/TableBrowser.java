package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.dbunit.meta.TableMetaDataRepository;
import com.link_intersystems.dbunit.sql.statement.SqlStatement;
import com.link_intersystems.dbunit.table.TableList;
import com.link_intersystems.jdbc.ConnectionMetaData;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.database.CachedResultSetTable;
import org.dbunit.database.ForwardOnlyResultSetTable;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableBrowser {

    public static IDataSet browse(IDatabaseConnection databaseConnection, BrowseTable tableBrowseRef) throws DataSetException {
        TableBrowser tableBrowser = new TableBrowser(databaseConnection);
        tableBrowser.browse(tableBrowseRef);
        return tableBrowser.getDataSet();
    }

    private TableList tableList = new TableList();

    private final IDatabaseConnection databaseConnection;
    private final TableMetaDataRepository tableMetaDataRepository;
    private final ConnectionMetaData connectionMetaData;

    private DefaultBrowseTableSqlFactory tableBrowseSqlFactory;

    public TableBrowser(IDatabaseConnection databaseConnection) throws DataSetException {
        this.databaseConnection = databaseConnection;
        tableMetaDataRepository = new TableMetaDataRepository(databaseConnection);
        try {
            connectionMetaData = new ConnectionMetaData(databaseConnection.getConnection());
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    protected BrowseTableSqlFactory getTableBrowseSqlFactory() {
        if (tableBrowseSqlFactory == null) {
            tableBrowseSqlFactory = new DefaultBrowseTableSqlFactory(connectionMetaData);
        }
        return tableBrowseSqlFactory;
    }


    public void browse(BrowseTable tableBrowseRef) throws DataSetException {
        SqlStatement sqlStatement = createSqlStatement(tableBrowseRef);
        browse(tableBrowseRef, sqlStatement);
    }

    protected SqlStatement createSqlStatement(BrowseTable tableBrowseRef) {
        BrowseTableSqlFactory tableBrowseSqlFactory = getTableBrowseSqlFactory();
        return tableBrowseSqlFactory.createSqlStatement(tableBrowseRef);
    }

    private void browse(BrowseTable targetTableRef, SqlStatement sqlStatement) throws DataSetException {
        try {
            Connection connection = databaseConnection.getConnection();
            ITable targetTable = sqlStatement.processResultSet(connection, rs -> {
                ResultSetMetaData metaData = rs.getMetaData();
                String tableName = metaData.getTableName(1);

                ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);
                ForwardOnlyResultSetTable forwardOnlyResultSetTable = new ForwardOnlyResultSetTable(tableMetaData, rs);
                return new CachedResultSetTable(forwardOnlyResultSetTable);
            });

            tableList.add(targetTable);

            List<BrowseTableReference> browseReferences = targetTableRef.getReferences();

            for (BrowseTableReference browseReference : browseReferences) {
                browseRef(targetTable, browseReference);
            }

        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private void browseRef(ITable sourceTable, BrowseTableReference targetBrowseReference) throws SQLException {
        BrowseTable targetTableRef = targetBrowseReference.getTargetBrowseTable();

        try {
            BrowseTableSqlFactory tableBrowseSqlFactory = getTableBrowseSqlFactory();

            SqlStatement sqlStatement = tableBrowseSqlFactory.createSqlStatement(sourceTable, targetBrowseReference);

            browse(targetTableRef, sqlStatement);
        } catch (Exception e) {
            String msg = MessageFormat.format("Can not browse from source table ''{0}'' to target table ''{1}''", sourceTable.getTableMetaData().getTableName(), targetTableRef.getTableName());
            throw new TableBrowseException(msg, e);
        }
    }

    public IDataSet getDataSet() {
        try {
            tableList.pack();
            return new DefaultDataSet(tableList.toArray(new ITable[0]));
        } catch (AmbiguousTableNameException e) {
            throw new RuntimeException("Please report a bug. This should not happen.", e);
        }
    }

}
