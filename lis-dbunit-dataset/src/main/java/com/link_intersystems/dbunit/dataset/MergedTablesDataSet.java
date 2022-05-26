package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.table.DistinctCompositeTable;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.*;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MergedTablesDataSet extends AbstractDataSet {

    private List<ITable> tableList;

    private IDataSet mergedDataSet;

    public MergedTablesDataSet(List<ITable> tableList) {
        this.tableList = tableList;
    }

    public IDataSet getMergedDataSet() {
        if (mergedDataSet == null) {
            LinkedHashMap<ITableMetaData, ITable> uniqueTables = new LinkedHashMap<>();

            for (ITable table : tableList) {
                ITableMetaData tableMetaData = table.getTableMetaData();
                ITable effectiveTable = uniqueTables.get(tableMetaData);

                if (effectiveTable == null) {
                    effectiveTable = table;
                } else {
                    try {
                        effectiveTable = new DistinctCompositeTable(effectiveTable, table);
                    } catch (DataSetException e) {
                        throw new IllegalArgumentException(e);
                    }
                }

                uniqueTables.put(tableMetaData, effectiveTable);
            }

            try {
                mergedDataSet = new DefaultDataSet(uniqueTables.values().toArray(new ITable[0]));
            } catch (AmbiguousTableNameException e) {
                throw new RuntimeException(e);
            }
        }

        return mergedDataSet;
    }

    @Override
    protected ITableIterator createIterator(boolean b) throws DataSetException {
        IDataSet mergedDataSet = getMergedDataSet();
        return b ? mergedDataSet.reverseIterator() : mergedDataSet.iterator();
    }
}
