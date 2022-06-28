package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.MergedDataSet;
import com.link_intersystems.dbunit.table.TableList;
import com.link_intersystems.dbunit.table.TableReferenceTraversal;
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
    private TableReferenceTraversal tableReferenceTraversal;

    public ConsistentDataSet(TableReferenceTraversal tableReferenceTraversal, ITable... tables) throws DataSetException {
        this(new DefaultDataSet(requireNonNull(tables)), tableReferenceTraversal);
    }

    public ConsistentDataSet(IDataSet sourceDataSet, TableReferenceTraversal tableReferenceTraversal) {
        this.sourceDataSet = requireNonNull(sourceDataSet);
        this.tableReferenceTraversal = tableReferenceTraversal;
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

            TableList outgoingTables = tableReferenceTraversal.traverseOutgoingReferences(table);
            List<ITable> filteredOutgoingTables = outgoingTables.stream().filter(uniqueTables::add).collect(toList());
            filteredOutgoingTables.forEach(tables::offer);
        }

        return resultDataSet;
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

