package com.link_intersystems.dbunit.dataset.browser;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnReferenceAssertion {
    private BrowseTable sourceTable;
    private String referencedTableName;
    private String[] sourceColumns;

    ColumnReferenceAssertion(BrowseTable sourceTable, String referencedTableName, String[] sourceColumns) {
        this.sourceTable = sourceTable;
        this.referencedTableName = referencedTableName;
        this.sourceColumns = sourceColumns;
    }

    public void references(String... targetColumns) {
        List<BrowseTableReference> references = sourceTable.getReferences();


        for (BrowseTableReference reference : references) {
            BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
            String targetTableName = targetBrowseTable.getTableName();
            if (targetTableName.equals(referencedTableName)) {
                String[] referenceSourceColumns = reference.getSourceColumns();
                String[] referenceTargetColumns = reference.getTargetColumns();

                boolean isReferencedByColumns = Arrays.equals(sourceColumns, referenceSourceColumns) &&
                        Arrays.equals(targetColumns, referenceTargetColumns);
                if (isReferencedByColumns) {
                    return;
                }
            }
        }

        String msg = MessageFormat.format("No reference from ''{0}'' to ''{1}'' found by colums {2} -> {3}",
                sourceTable.getTableName(), referencedTableName, Arrays.asList(sourceColumns), Arrays.asList(targetColumns));
        fail(msg);


    }
}
