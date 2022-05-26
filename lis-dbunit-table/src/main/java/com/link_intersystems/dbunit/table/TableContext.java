package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.meta.TableReference;
import org.dbunit.dataset.ITable;

import java.util.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableContext extends AbstractList<ITable> {

    private List<ITable> tables = new ArrayList<>();
    private Set<TableReference> traversedDependencies = new HashSet<>();

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

    public boolean follow(TableReference dependency) {
        return traversedDependencies.add(dependency);
    }

}
