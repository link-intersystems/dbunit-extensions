package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.Dependency;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;

import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableContext extends AbstractList<ITable> {

    private List<ITable> tables = new ArrayList<>();
    private Set<Dependency> traversedDependencies = new HashSet<>();

    public Map<String, ITable> toMap() {
        LinkedHashMap<String, ITable> map = new LinkedHashMap<>();

        stream().forEach(t -> map.put(t.getTableMetaData().getTableName(), t));

        return map;
    }


    public ITable getByName(String name) {
        return stream().filter(t -> {
            if (t == null) {
                return false;
            }
            return t.getTableMetaData().getTableName().equals(name);
        }).findFirst().orElse(null);
    }

    @Override
    public void add(int index, ITable element) {
        ITable effectiveTable = getEffectiveTable(element);
        if (effectiveTable != element) {
            String tableName = element.getTableMetaData().getTableName();
            ITable existingTable = getByName(tableName);
            int replaceTableIndex = indexOf(existingTable);
            tables.add(index, effectiveTable);
            int removeIndex = replaceTableIndex < index ? replaceTableIndex : replaceTableIndex + 1;
            remove(removeIndex);
        } else {
            tables.add(index, effectiveTable);
        }
    }

    private ITable getEffectiveTable(ITable element) {
        if (element == null) {
            return null;
        }

        String tableName = element.getTableMetaData().getTableName();
        ITable existingTable = getByName(tableName);
        if (existingTable != null) {
            try {
                element = new DistinctCompositeTable(existingTable, element);
            } catch (DataSetException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return element;
    }

    @Override
    public ITable set(int index, ITable element) {
        ITable effectiveTable = getEffectiveTable(element);
        if (effectiveTable != element) {
            String tableName = element.getTableMetaData().getTableName();
            ITable existingTable = getByName(tableName);
            int replaceTableIndex = indexOf(existingTable);
            ITable previous = tables.set(index, effectiveTable);
            int removeIndex = replaceTableIndex < index ? replaceTableIndex : replaceTableIndex + 1;
            tables.set(removeIndex, null);
            return previous;
        } else {
            return tables.set(index, effectiveTable);
        }
    }

    @Override
    public ITable remove(int index) {
        return tables.remove(index);
    }

    @Override
    public ITable get(int index) {
        return tables.get(index);
    }

    @Override
    public int size() {
        return tables.size();
    }

    public ListSnapshot<ITable> getSnapshot() {
        return new ListSnapshot(this);
    }

    public boolean follow(Dependency dependency) {
        return traversedDependencies.add(dependency);
    }

    public List<ITable> getUniqueTables() {
        return this;
    }
}
