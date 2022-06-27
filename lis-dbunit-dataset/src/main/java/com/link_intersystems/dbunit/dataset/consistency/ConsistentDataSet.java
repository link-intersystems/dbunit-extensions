package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.MergedDataSet;
import com.link_intersystems.dbunit.table.TableList;
import com.link_intersystems.dbunit.table.TableReferenceLoader;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.*;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDataSet extends AbstractDataSet {

    private final MergedDataSet mergedDataSet;

    public ConsistentDataSet(ITable rootTable, IDatabaseConnection databaseConnection) throws DataSetException {
        this(new DefaultDataSet(rootTable), databaseConnection);
    }

    public ConsistentDataSet(IDataSet dataSet, IDatabaseConnection databaseConnection) throws DataSetException {
        ITableIterator iterator = dataSet.iterator();


        TableReferenceLoader entityDependencyLoader = new TableReferenceLoader(databaseConnection);

        TableList tableList = new TableList();

        while (iterator.next()) {
            ITable table = iterator.getTable();
            tableList.addAll(loadOutgoingTables(entityDependencyLoader, table));
            tableList.pack();
        }

        mergedDataSet = new MergedDataSet(tableList);
    }

    private TableList loadOutgoingTables(TableReferenceLoader entityDependencyLoader, ITable table) throws DataSetException {
        TableList tableList = loadTables(entityDependencyLoader, table);
        tableList.add(0, table);
        return tableList;
    }

    private TableList loadTables(TableReferenceLoader entityDependencyLoader, ITable table) throws DataSetException {
        TableList loadedTables = entityDependencyLoader.loadOutgoingReferences(table);


        int size = loadedTables.size();
        for (int i = 0; i < size; i++) {
            ITable outgoingTable = loadedTables.get(i);
            List<ITable> subsequentTables = loadTables(entityDependencyLoader, outgoingTable);
            loadedTables.addAll(subsequentTables);
        }

        return loadedTables;
    }

    @Override
    protected ITableIterator createIterator(boolean b) throws DataSetException {
        return b ? mergedDataSet.reverseIterator() : mergedDataSet.iterator();
    }
}

