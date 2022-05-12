package com.link_intersystems.dbunit.dataset.jdbc;

import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcForeignKey extends AbstractList<JdbcForeignKeyEntry> {

    private List<JdbcForeignKeyEntry> foreignKeyEntryList = new ArrayList<>();

    public JdbcForeignKey(List<JdbcForeignKeyEntry> foreignKeyEntryList) {
        if (foreignKeyEntryList.isEmpty()) {
            throw new IllegalArgumentException("foreignKeyEntryList must not be empty");
        }

        JdbcForeignKeyEntry mainFkEntry = null;
        for (JdbcForeignKeyEntry jdbcForeignKeyEntry : foreignKeyEntryList) {
            if (mainFkEntry == null) {
                mainFkEntry = jdbcForeignKeyEntry;
            } else if (!mainFkEntry.isSameForeignKey(jdbcForeignKeyEntry)) {
                String msg = MessageFormat.format("foreignKeyEntryList contains different foreign keys: {0} != {1}", mainFkEntry, jdbcForeignKeyEntry);
                throw new IllegalArgumentException(msg);
            }


        }
        this.foreignKeyEntryList.addAll(foreignKeyEntryList);
    }

    @Override
    public JdbcForeignKeyEntry get(int index) {
        return foreignKeyEntryList.get(index);
    }

    @Override
    public int size() {
        return foreignKeyEntryList.size();
    }

    public String getName() {
        return get(0).getFkName();
    }
}
