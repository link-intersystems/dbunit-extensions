package com.link_intersystems.dbunit.table;

import org.dbunit.database.CyclicTablesDependencyException;
import org.dbunit.database.DatabaseSequenceFilter;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.DataSetException;
import org.dbunit.util.search.SearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseTableOrder implements TableOrder {

    private static Logger logger = LoggerFactory.getLogger(DatabaseTableOrder.class);

    private IDatabaseConnection databaseConnection;

    public DatabaseTableOrder(IDatabaseConnection databaseConnection) {
        this.databaseConnection = requireNonNull(databaseConnection);
    }

    @Override
    public String[] orderTables(String... tableNames) throws DataSetException {
        return sortTableNames(databaseConnection, tableNames);
    }

    // Code below this line is taken from org.dbunit.database.DatabaseSequenceFilter
    static String[] sortTableNames(IDatabaseConnection connection, String[] tableNames) throws DataSetException {
        logger.debug("sortTableNames(connection={}, tableNames={}) - start", connection, tableNames);
        HashMap dependencies = new HashMap();

        try {
            for (int i = 0; i < tableNames.length; ++i) {
                String tableName = tableNames[i];
                DependencyInfo info = getDependencyInfo(connection, tableName);
                dependencies.put(tableName, info);
            }
        } catch (SearchException var6) {
            throw new DataSetException("Exception while searching the dependent tables.", var6);
        }

        Iterator iterator = dependencies.values().iterator();

        while (iterator.hasNext()) {
            DependencyInfo info = (DependencyInfo) iterator.next();
            info.checkCycles();
        }

        return sort(tableNames, dependencies);
    }

    private static String[] sort(String[] tableNames, Map dependencies) {
        logger.debug("sort(tableNames={}, dependencies={}) - start", tableNames, dependencies);
        boolean reprocess = true;
        List tmpTableNames = Arrays.asList(tableNames);
        LinkedList sortedTableNames = null;

        while (reprocess) {
            sortedTableNames = new LinkedList();
            Iterator i = tmpTableNames.iterator();

            while (i.hasNext()) {
                boolean foundDependentInSortedTableNames = false;
                String tmpTable = (String) i.next();
                DependencyInfo tmpTableDependents = (DependencyInfo) dependencies.get(tmpTable);
                int sortedTableIndex = -1;
                Iterator k = sortedTableNames.iterator();

                while (k.hasNext()) {
                    String sortedTable = (String) k.next();
                    if (tmpTableDependents.containsDirectDependsOn(sortedTable)) {
                        sortedTableIndex = sortedTableNames.indexOf(sortedTable);
                        foundDependentInSortedTableNames = true;
                        break;
                    }
                }

                if (foundDependentInSortedTableNames) {
                    if (sortedTableIndex < 0) {
                        throw new IllegalStateException("sortedTableIndex should be 0 or greater, but is " + sortedTableIndex);
                    }

                    sortedTableNames.add(sortedTableIndex, tmpTable);
                } else {
                    sortedTableNames.add(tmpTable);
                }
            }

            if (tmpTableNames.equals(sortedTableNames)) {
                reprocess = false;
            } else {
                tmpTableNames = (List) ((LinkedList) sortedTableNames).clone();
            }
        }

        return (String[]) sortedTableNames.toArray(new String[0]);
    }

    private static DependencyInfo getDependencyInfo(IDatabaseConnection connection, String tableName) throws SearchException {
        logger.debug("getDependencyInfo(connection={}, tableName={}) - start", connection, tableName);
        String[] allDependentTables = TablesDependencyHelper.getDependentTables(connection, tableName);
        String[] allDependsOnTables = TablesDependencyHelper.getDependsOnTables(connection, tableName);
        Set allDependentTablesSet = new HashSet(Arrays.asList(allDependentTables));
        Set allDependsOnTablesSet = new HashSet(Arrays.asList(allDependsOnTables));
        allDependentTablesSet.remove(tableName);
        allDependsOnTablesSet.remove(tableName);
        Set directDependsOnTablesSet = TablesDependencyHelper.getDirectDependsOnTables(connection, tableName);
        Set directDependentTablesSet = TablesDependencyHelper.getDirectDependentTables(connection, tableName);
        directDependsOnTablesSet.remove(tableName);
        directDependentTablesSet.remove(tableName);
        DependencyInfo info = new DependencyInfo(tableName, directDependsOnTablesSet, directDependentTablesSet, allDependsOnTablesSet, allDependentTablesSet);
        return info;
    }

    static class DependencyInfo {
        private static final Logger logger = LoggerFactory.getLogger(DatabaseSequenceFilter.class);
        private String tableName;
        private Set allTableDependsOn;
        private Set allTableDependent;
        private Set directDependsOnTablesSet;
        private Set directDependentTablesSet;

        public DependencyInfo(String tableName, Set directDependsOnTablesSet, Set directDependentTablesSet, Set allTableDependsOn, Set allTableDependent) {
            this.directDependsOnTablesSet = directDependsOnTablesSet;
            this.directDependentTablesSet = directDependentTablesSet;
            this.allTableDependsOn = allTableDependsOn;
            this.allTableDependent = allTableDependent;
            this.tableName = tableName;
        }

        public boolean containsDirectDependent(String tableName) {
            return this.directDependentTablesSet.contains(tableName);
        }

        public boolean containsDirectDependsOn(String tableName) {
            return this.directDependsOnTablesSet.contains(tableName);
        }

        public String getTableName() {
            return this.tableName;
        }

        public Set getAllTableDependsOn() {
            return this.allTableDependsOn;
        }

        public Set getAllTableDependent() {
            return this.allTableDependent;
        }

        public Set getDirectDependsOnTablesSet() {
            return this.directDependsOnTablesSet;
        }

        public Set getDirectDependentTablesSet() {
            return this.directDependentTablesSet;
        }

        public void checkCycles() throws CyclicTablesDependencyException {
            logger.debug("checkCycles() - start");
            Set intersect = new HashSet(this.allTableDependsOn);
            intersect.retainAll(this.allTableDependent);
            if (!intersect.isEmpty()) {
                throw new CyclicTablesDependencyException(this.tableName, intersect);
            }
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("DependencyInfo[");
            sb.append("table=").append(this.tableName);
            sb.append(", directDependsOn=").append(this.directDependsOnTablesSet);
            sb.append(", directDependent=").append(this.directDependentTablesSet);
            sb.append(", allDependsOn=").append(this.allTableDependsOn);
            sb.append(", allDependent=").append(this.allTableDependent);
            sb.append("]");
            return sb.toString();
        }
    }
}
