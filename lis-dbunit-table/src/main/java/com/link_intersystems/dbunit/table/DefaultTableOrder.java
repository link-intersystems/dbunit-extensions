package com.link_intersystems.dbunit.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultTableOrder implements TableOrder {

    private List<String> tableOrder;

    private class TableOrderComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            int o1Index = tableOrder.indexOf(o1);
            int o2Index = tableOrder.indexOf(o2);

            return o1Index - o2Index;
        }
    }

    private TableOrderComparator tableOrderComparator = new TableOrderComparator();

    public DefaultTableOrder(String... tableOrder) {
        this(Arrays.asList(tableOrder));
    }

    public DefaultTableOrder(List<String> tableOrder) {
        this.tableOrder = requireNonNull(tableOrder);
    }

    @Override
    public String[] orderTables(String... tableNames) {
        List<String> tableNameList = Arrays.asList(tableNames);
        tableNameList.sort(tableOrderComparator);
        return tableNameList.toArray(new String[0]);
    }
}
