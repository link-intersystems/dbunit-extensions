package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.DataSetException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableOrder {

    default TableOrder reverse() {
        return tableNames -> {
            List<String> tableNameList = Arrays.asList(TableOrder.this.orderTables(tableNames));
            Collections.reverse(tableNameList);
            return tableNameList.toArray(new String[0]);
        };
    }

    String[] orderTables(String... tableNames) throws DataSetException;
}
