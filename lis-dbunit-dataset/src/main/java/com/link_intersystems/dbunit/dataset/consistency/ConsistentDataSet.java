package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.MergedDataSet;
import com.link_intersystems.dbunit.table.TableList;
import com.link_intersystems.dbunit.table.TableReferenceLoader;
import org.dbunit.dataset.*;

import java.util.*;

import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ConsistentDataSet extends AbstractDataSet {

    private IDataSet consistentDataSet;
    private IDataSet sourceDataSet;
    private TableReferenceLoader tableReferenceLoader;

    public ConsistentDataSet(TableReferenceLoader tableReferenceLoader, ITable... tables) throws DataSetException {
        this(new DefaultDataSet(requireNonNull(tables)), tableReferenceLoader);
    }

    public ConsistentDataSet(IDataSet sourceDataSet, TableReferenceLoader tableReferenceLoader) {
        this.sourceDataSet = requireNonNull(sourceDataSet);
        this.tableReferenceLoader = tableReferenceLoader;
    }

    private IDataSet getConsistentDataSet() throws DataSetException {
        if (consistentDataSet == null) {
            Queue<ITable> tables = queueTables(sourceDataSet);

            TableList resultDataSet = loadTables(tables);

            consistentDataSet = new MergedDataSet(resultDataSet);
        }
        return consistentDataSet;
    }

    private TableList loadTables(Queue<ITable> tables) throws DataSetException {
        TableList resultDataSet = new TableList();

        Set<ITable> uniqueTables = newSetFromMap(new IdentityHashMap<>());

        while (!tables.isEmpty()) {
            ITable table = tables.poll();
            resultDataSet.add(table);

            List<ITable> outgoingReferencedTables = loadOutgoingReferences(uniqueTables, table);
            outgoingReferencedTables.forEach(tables::offer);
        }

        return resultDataSet;
    }

    private List<ITable> loadOutgoingReferences(Set<ITable> uniqueTables, ITable table) throws DataSetException {
        TableList loadedTables = tableReferenceLoader.loadOutgoingReferences(table);
        return loadedTables.stream().filter(uniqueTables::add).collect(toList());
    }

    private Queue<ITable> queueTables(IDataSet dataSet) throws DataSetException {
        Queue<ITable> tablesToLoad = new LinkedList<>();

        ITableIterator iterator = dataSet.iterator();
        while (iterator.next()) {
            ITable table = iterator.getTable();
            tablesToLoad.offer(table);
        }

        return tablesToLoad;
    }

    @Override
    protected ITableIterator createIterator(boolean b) throws DataSetException {
        IDataSet consistentDataSet = getConsistentDataSet();
        return b ? consistentDataSet.reverseIterator() : consistentDataSet.iterator();
    }
}

