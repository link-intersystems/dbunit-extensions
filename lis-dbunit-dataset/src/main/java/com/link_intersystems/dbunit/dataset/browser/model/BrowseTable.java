package com.link_intersystems.dbunit.dataset.browser.model;

import com.link_intersystems.jdbc.TableReference;
import com.link_intersystems.jdbc.TableReferenceList;
import com.link_intersystems.jdbc.TableReferenceMetaData;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

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

    public OngoingBrowseTable browse(String targetTableName) {
        BrowseTable targetBrowseTable = new BrowseTable(targetTableName);
        BrowseTableReference targetRefernce = new BrowseTableReference(targetBrowseTable);
        references.add(targetRefernce);
        return new OngoingBrowseTable(targetRefernce);
    }

    public BrowseTableCriteriaBuilder with(String columnName) {
        return new BrowseTableCriteriaBuilder(this, columnName);
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
                browse(outgoingReference);
            }
        }

        for (BrowseTableReference reference : getReferences()) {
            BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
            targetBrowseTable.makeConsistent(tableReferenceMetaData);
        }

    }

    public BrowseTable browse(TableReference tableReference) {
        if (tableReference.getTargetEdge().getTableName().equals(getTableName())) {
            tableReference = tableReference.reverse();
        }

        if (!tableReference.getSourceEdge().getTableName().equals(getTableName())) {
            String msg = MessageFormat.format("Can not browse table reference ''{0}'', " +
                            "because neither the source nor the target edge points to this {1}'s table name ''{2}''",
                    tableReference,
                    BrowseTable.class.getSimpleName(),
                    getTableName());
            throw new IllegalArgumentException(msg);
        }
        TableReference.Edge targetEdge = tableReference.getTargetEdge();
        String targetTableName = targetEdge.getTableName();
        TableReference.Edge sourceEdge = tableReference.getSourceEdge();

        return browse(targetTableName)
                .on(sourceEdge.getColumns())
                .references(targetEdge.getColumns());
    }

    private boolean isBrowsed(TableReference outgoingReference) {
        List<BrowseTableReference> references = getReferences();

        for (BrowseTableReference reference : references) {
            if (isEqual(outgoingReference, reference)) {
                return true;
            }
        }

        return false;
    }

    private boolean isEqual(TableReference outgoingReference, BrowseTableReference reference) {
        TableReference.Edge targetEdge = outgoingReference.getTargetEdge();

        BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
        String targetTableName = targetBrowseTable.getTableName();

        if (!targetEdge.getTableName().equals(targetTableName)) {
            return false;
        }


        List<String> sourceColumns = asList(reference.getSourceColumns());
        List<String> targetColumns = asList(reference.getTargetColumns());

        if (sourceColumns.isEmpty() && targetColumns.isEmpty()) {
            return true;
        }


        TableReference.Edge sourceEdge = outgoingReference.getSourceEdge();
        if (!sourceEdge.getColumns().equals(sourceColumns)) {
            return false;
        }

        return targetEdge.getColumns().equals(targetColumns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrowseTable that = (BrowseTable) o;
        return Objects.equals(tableName, that.tableName) && Objects.equals(references, that.references) && Objects.equals(tableCriteria, that.tableCriteria);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, references, tableCriteria);
    }
}
