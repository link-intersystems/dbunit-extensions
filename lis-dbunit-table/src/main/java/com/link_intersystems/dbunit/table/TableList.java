package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

import java.util.*;

/**
 * A list of tables that can be packed to make the tables unique using {@link MergedTable}s.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableList extends AbstractList<ITable> {

    /**
     * Merges the given tables by their {@link ITableMetaData} using a {@link MergedTable}.
     *
     * @param tables the tables to merge.
     * @return a list of unique {@link ITable} that have been merged using a {@link MergedTable}.
     */
    public static List<ITable> merge(List<ITable> tables) {
        LinkedHashMap<String, ITable> mergedTables = new LinkedHashMap<>();

        for (ITable table : tables) {
            ITableMetaData tableMetaData = table.getTableMetaData();
            String tableName = tableMetaData.getTableName();
            ITable effectiveTable = mergedTables.get(tableName);

            if (effectiveTable == null) {
                effectiveTable = table;
            } else {
                try {
                    effectiveTable = new MergedTable(effectiveTable, table);
                } catch (DataSetException e) {
                    throw new IllegalArgumentException(e);
                }
            }

            mergedTables.put(tableName, effectiveTable);
        }

        return new ArrayList<>(mergedTables.values());
    }

    private List<ITable> tables;

    public TableList() {
        this(Collections.emptyList());
    }

    public TableList(List<ITable> tables) {
        this.tables = new ArrayList<>(tables);
    }

    /**
     * Packs this {@link TableList} by merging duplicate {@link ITable}s into one {@link MergedTable}.
     */
    public void pack() {
        tables = merge(tables);
    }

    @Override
    public void add(int index, ITable element) {
        tables.add(index, element);
    }

    @Override
    public ITable set(int index, ITable element) {
        return tables.set(index, element);
    }

    @Override
    public ITable remove(int index) {
        return tables.remove(index);
    }

    public ITable getByName(String tableName) {
        return tables.stream().filter(Objects::nonNull)
                .filter(t -> t.getTableMetaData().getTableName().equals(tableName))
                .findFirst()
                .orElse(null);
    }

    @Override
    public ITable get(int index) {
        return tables.get(index);
    }

    @Override
    public int size() {
        return tables.size();
    }
}
