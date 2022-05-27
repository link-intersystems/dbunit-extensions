package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.table.TableList;
import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.*;

import java.util.List;

/**
 * A {@link IDataSet} that merges multiple tables of the same name into one eliminates row duplicates according to the
 * primary key specification of the {@link ITableMetaData} of the tables.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MergedDataSet extends AbstractDataSet {

    private TableList tableList;

    private IDataSet mergedDataSet;

    public MergedDataSet(List<ITable> tableList) {
        this.tableList = new TableList(tableList);
    }

    public IDataSet getMergedDataSet() {
        if (mergedDataSet == null) {
            tableList.pack();

            try {
                mergedDataSet = new DefaultDataSet(tableList.toArray(new ITable[0]));
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
