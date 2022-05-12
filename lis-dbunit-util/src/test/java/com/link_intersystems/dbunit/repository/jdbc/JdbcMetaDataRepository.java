package com.link_intersystems.dbunit.repository.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcMetaDataRepository {

    private List<JdbcTableMetaData> tableMetaDataList;
    private Map<String, List<JdbcColumnMetaData>> columnMetaDataListByTableName = new HashMap<>();

    private Connection connection;
    private String[] tableTypes;

    private JdbcContext context;


    public JdbcMetaDataRepository(Connection connection) {
        this(connection, new String[]{"TABLE"});
    }

    public JdbcMetaDataRepository(Connection connection, String... tableTypes) {
        this(connection, null, tableTypes);
    }

    public JdbcMetaDataRepository(Connection connection, JdbcContext context, String... tableTypes) {
        this.connection = connection;
        this.context = context;
        this.tableTypes = tableTypes;
    }

    public List<JdbcTableMetaData> getTableMetaDataList() throws SQLException {
        if (tableMetaDataList == null) {
            ScopedDatabaseMetaData metaData = getScopedDatabaseMetaData();
            ResultSet tablesResultSet = metaData.getTables("%", tableTypes);
            tableMetaDataList = mapResultSet(tablesResultSet, JdbcTableMetaData::new);
        }
        return tableMetaDataList;
    }

    private ScopedDatabaseMetaData getScopedDatabaseMetaData() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        JdbcContext context = getContext();
        return new ScopedDatabaseMetaData(metaData, context.getCatalog(), context.getSchema());
    }

    private JdbcContext getContext() throws SQLException {
        if (context == null) {
            String catalog = connection.getCatalog();
            String schema = connection.getSchema();
            context = new JdbcContext(catalog, schema);
        }
        return context;
    }

    public JdbcTableMetaData getTableMetaData(String tableName) throws SQLException {
        return getTableMetaDataList().stream().filter(jtmd -> jtmd.getTableName().equals(tableName)).findFirst().orElse(null);
    }

    public List<JdbcColumnMetaData> getColumnMetaDataList(JdbcTableMetaData jdbcTableMetaData) throws SQLException {
        return getColumnMetaDataList(jdbcTableMetaData.getTableName());
    }

    public List<JdbcColumnMetaData> getColumnMetaDataList(String tableName) throws SQLException {
        if (!columnMetaDataListByTableName.containsKey(tableName)) {
            List<JdbcColumnMetaData> columnMetaDatas = createColumnMetaData(tableName);
            columnMetaDataListByTableName.put(tableName, columnMetaDatas);
        }
        return columnMetaDataListByTableName.get(tableName);

    }

    private List<JdbcColumnMetaData> createColumnMetaData(String tableName) throws SQLException {
        ScopedDatabaseMetaData scopedDatabaseMetaData = getScopedDatabaseMetaData();
        ResultSet columnsMetaDataResultSet = scopedDatabaseMetaData.getColumns(tableName);

        return mapResultSet(columnsMetaDataResultSet, JdbcColumnMetaData::new);
    }

    public List<JdbcForeignKey> getExportedKeys(String tableName) throws SQLException {
        ScopedDatabaseMetaData scopedDatabaseMetaData = getScopedDatabaseMetaData();
        ResultSet resultSet = scopedDatabaseMetaData.getExportedKeys(tableName);
        List<JdbcForeignKeyEntry> jdbcForeignKeyEntries = mapResultSet(resultSet, JdbcForeignKeyEntry::new);

        return mapToForeignKeys(jdbcForeignKeyEntries);
    }

    private List<JdbcForeignKey> mapToForeignKeys(List<JdbcForeignKeyEntry> jdbcForeignKeyEntries) {
        Map<String, List<JdbcForeignKeyEntry>> foreignKeys = jdbcForeignKeyEntries.stream().collect(Collectors.groupingBy(JdbcForeignKeyEntry::getFkName));
        return foreignKeys.values().stream().map(JdbcForeignKey::new).collect(Collectors.toList());
    }

    public List<JdbcForeignKey> getImportedKeys(String tableName) throws SQLException {
        ScopedDatabaseMetaData scopedDatabaseMetaData = getScopedDatabaseMetaData();
        ResultSet resultSet = scopedDatabaseMetaData.getImportedKeys(tableName);
        List<JdbcForeignKeyEntry> jdbcForeignKeyEntries = mapResultSet(resultSet, JdbcForeignKeyEntry::new);
        return mapToForeignKeys(jdbcForeignKeyEntries);
    }

    @FunctionalInterface
    private static interface ElementFactory<T> {

        public T apply(ResultSet resultSet) throws SQLException;
    }

    private <T> List<T> mapResultSet(ResultSet resultSet, ElementFactory<T> elementFactory) throws SQLException {
        List<T> mappedResultSet = new ArrayList<>();

        while (resultSet.next()) {
            T element = elementFactory.apply(resultSet);
            mappedResultSet.add(element);
        }

        return mappedResultSet;
    }
}
