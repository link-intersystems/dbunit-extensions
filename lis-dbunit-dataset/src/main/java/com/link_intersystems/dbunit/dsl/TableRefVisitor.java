package com.link_intersystems.dbunit.dsl;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TableRefVisitor {
    void visitRootTable(String tableName);

    void visit(BrowseTable sourceTableRef, BrowseTableReference browseNode);
}
