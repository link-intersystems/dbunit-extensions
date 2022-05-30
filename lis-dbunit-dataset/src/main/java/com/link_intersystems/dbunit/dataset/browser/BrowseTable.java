package com.link_intersystems.dbunit.dataset.browser;

import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceList;
import com.link_intersystems.jdbc.TableReferenceMetaData;
import org.dbunit.dataset.DataSetException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BrowseTable {

    private String tableName;
    private List<BrowseTableReference> references = new ArrayList<>();
    private TableCriteria tableCriteria;

    public BrowseTable(String tableName) {
        this.tableName = tableName;
    }

    public OngoingBrowseTable browse(String tableName) {
        return new OngoingBrowseTable(this, tableName);
    }

    public BrowseTable browseNatural(String joinTableName) {
        return browse(joinTableName).natural();
    }

    void addBrowse(BrowseTableReference join) {
        references.add(join);
    }

    public ColumnCriteriaBuilder with(String columnName) {
        return new ColumnCriteriaBuilder(this, columnName);
    }

    void addCriterion(TableCriterion tableCriterion) {
        if (tableCriteria == null) {
            tableCriteria = new TableCriteria();
        }
        tableCriteria.addCriterion(tableCriterion);
    }

    public String getTableName() {
        return tableName;
    }

    public TableCriteria getCriteria() {
        return tableCriteria;
    }

    public List<BrowseTableReference> getReferences() {
        return new ArrayList<>(references);
    }

    public void makeConsistent(TableReferenceMetaData tableReferenceMetaData) throws SQLException {
        String tableName = getTableName();

        TableReferenceList outgoingReferences = tableReferenceMetaData.getOutgoingReferences(tableName);


        for (TableReference outgoingReference : outgoingReferences) {
            if (!isBrowsed(outgoingReference)) {
                TableReference.Edge targetEdge = outgoingReference.getTargetEdge();
                String targetTableName = targetEdge.getTableName();
                TableReference.Edge sourceEdge = outgoingReference.getSourceEdge();

                browse(targetTableName)
                        .on(sourceEdge.getColumns())
                        .references(targetEdge.getColumns());
            }
        }

        for (BrowseTableReference reference : getReferences()) {
            BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
            targetBrowseTable.makeConsistent(tableReferenceMetaData);
        }

    }

    private boolean isBrowsed(TableReference outgoingReference) {
        List<BrowseTableReference> references = getReferences();

        for (BrowseTableReference reference : references) {
            if (isBrowsed(outgoingReference, reference)) {
                return true;
            }
        }

        return false;
    }

    private boolean isBrowsed(TableReference outgoingReference, BrowseTableReference reference) {
        TableReference.Edge targetEdge = outgoingReference.getTargetEdge();

        BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
        String targetTableName = targetBrowseTable.getTableName();
        if (!targetEdge.getTableName().equals(targetTableName)) {
            return false;
        }


        List<String> sourceColumns = Arrays.asList(reference.getSourceColumns());
        List<String> targetColumns = Arrays.asList(reference.getTargetColumns());
        if (sourceColumns.isEmpty() && targetColumns.isEmpty()) {
            return true;
        }


        TableReference.Edge sourceEdge = outgoingReference.getSourceEdge();
        if (!sourceEdge.getColumns().equals(sourceColumns)) {
            return false;
        }

        return targetEdge.getColumns().equals(targetColumns);
    }
}
