package com.link_intersystems.dbunit.dataset.meta;

import com.link_intersystems.dbunit.dataset.jdbc.ColumnDescription;
import com.link_intersystems.dbunit.dataset.jdbc.JdbcForeignKey;
import com.link_intersystems.dbunit.dataset.jdbc.JdbcForeignKeyEntry;
import com.link_intersystems.dbunit.dataset.jdbc.JdbcMetaDataRepository;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableDependencyRepository {

    private IDatabaseConnection databaseConnection;
    private TableMetaDataRepository tableMetaDataRepository;

    private JdbcMetaDataRepository jdbcMetaDataRepository;

    public TableDependencyRepository(IDatabaseConnection databaseConnection, TableMetaDataRepository tableMetaDataRepository) {
        this.databaseConnection = databaseConnection;
        this.tableMetaDataRepository = tableMetaDataRepository;
    }

    public List<Dependency> getIncomingDependencies(String tableName) throws DataSetException {
        try {
            JdbcMetaDataRepository jdbcMetaDataRepository = getJdbcMetaDataRepository();
            List<JdbcForeignKey> exportedKeys = jdbcMetaDataRepository.getExportedKeys(tableName);
            return mapToDependency(exportedKeys);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public List<Dependency> getOutgoingDependencies(String tableName) throws DataSetException {
        try {
            JdbcMetaDataRepository jdbcMetaDataRepository = getJdbcMetaDataRepository();
            List<JdbcForeignKey> exportedKeys = jdbcMetaDataRepository.getImportedKeys(tableName);
            return mapToDependency(exportedKeys);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    public JdbcMetaDataRepository getJdbcMetaDataRepository() throws DataSetException {
        if (jdbcMetaDataRepository == null) {
            try {
                Connection connection = databaseConnection.getConnection();
                jdbcMetaDataRepository = new JdbcMetaDataRepository(connection);
            } catch (SQLException e) {
                throw new DataSetException(e);
            }
        }
        return jdbcMetaDataRepository;
    }

    private List<Dependency> mapToDependency(List<JdbcForeignKey> foreignKeys) throws DataSetException {
        List<Dependency> dependencies = new ArrayList<>();
        for (JdbcForeignKey foreignKey : foreignKeys) {
            Dependency dependency = mapToDependency(foreignKey);
            dependencies.add(dependency);
        }
        return dependencies;
    }

    private Dependency mapToDependency(JdbcForeignKey foreignKey) throws DataSetException {
        List<TableColumn> pkColumns = getColumns(foreignKey, this::getPkColumn);
        List<TableColumn> fkColumns = getColumns(foreignKey, this::getFkColumn);

        String name = foreignKey.getName();

        return new Dependency(name, toEdge(fkColumns), toEdge(pkColumns));
    }

    private Dependency.Edge toEdge(List<TableColumn> tableColumns) throws DataSetException {
        Map<ITableMetaData, List<TableColumn>> grouped = tableColumns.stream().collect(Collectors.groupingBy(TableColumn::getTableMetaData));
        for (Map.Entry<ITableMetaData, List<TableColumn>> entry : grouped.entrySet()) {
            ITableMetaData tableMetaData = entry.getKey();
            List<Column> columns = entry.getValue().stream().map(TableColumn::getColumn).collect(Collectors.toList());
            return new Dependency.Edge(tableMetaData, columns);
        }
        throw new IllegalStateException();
    }

    @FunctionalInterface
    private static interface ColumnGetter {
        public TableColumn getColumn(JdbcForeignKeyEntry foreignKeyEntry) throws DataSetException;
    }

    private class TableColumn {
        private ITableMetaData tableMetaData;
        private Column column;

        public TableColumn(ITableMetaData tableMetaData, Column column) {
            this.tableMetaData = tableMetaData;
            this.column = column;
        }

        public ITableMetaData getTableMetaData() {
            return tableMetaData;
        }

        public Column getColumn() {
            return column;
        }
    }

    private List<TableColumn> getColumns(JdbcForeignKey foreignKey, ColumnGetter columnGetter) throws DataSetException {
        List<TableColumn> columns = new ArrayList<>();

        for (JdbcForeignKeyEntry entry : foreignKey) {
            TableColumn column = columnGetter.getColumn(entry);
            columns.add(column);
        }

        return columns;
    }

    private TableColumn getPkColumn(JdbcForeignKeyEntry foreignKeyEntry) throws DataSetException {
        return getColumn(foreignKeyEntry.getPkColumnDescription());
    }

    private TableColumn getFkColumn(JdbcForeignKeyEntry foreignKeyEntry) throws DataSetException {
        return getColumn(foreignKeyEntry.getFkColumnDescription());
    }

    private TableColumn getColumn(ColumnDescription columnDescription) throws DataSetException {
        String tableName = columnDescription.getTableName();
        ITableMetaData tableMetaData = tableMetaDataRepository.getTableMetaData(tableName);

        String columnName = columnDescription.getColumnName();
        Column column = Arrays.stream(tableMetaData.getColumns()).filter(c -> c.getColumnName().equals(columnName)).findFirst().orElse(null);
        if (column == null) {
            String msg = MessageFormat.format("{0} doesn't contain the column {1}.{2}", TableMetaDataRepository.class.getSimpleName(), tableName, columnName);
            throw new DataSetException(msg);
        }

        return new TableColumn(tableMetaData, column);
    }

}
