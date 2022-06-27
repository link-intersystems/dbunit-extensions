package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.table.TableList;
import org.dbunit.dataset.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * A {@link IDataSet} that merges multiple tables of the same name into one eliminates row duplicates according to the
 * primary key specification of the {@link ITableMetaData} of the tables.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MergedDataSet extends AbstractDataSet {

    private TableList tables;

    public MergedDataSet(ITable... tables) {
        this(asList(tables));
    }

    public MergedDataSet(List<ITable> tables) {
        this.tables = new TableList(tables);
        this.tables.pack();
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) {
        List<ITable> iterateTables = tables;

        if (reversed) {
            iterateTables = new ArrayList<>(tables);
            Collections.reverse(iterateTables);
        }

        return new DefaultTableIterator(iterateTables.toArray(new ITable[0]));
    }

}
