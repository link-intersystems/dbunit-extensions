package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import com.link_intersystems.dbunit.dataset.browser.model.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BinaryBrowseTableSerdes implements BrowseTableSerdes {

    @Override
    public void serialize(BrowseTable browseTable, OutputStream outputStream) throws Exception {
        ObjectOutputStream oos = new ObjectOutputStream(outputStream);

        write(browseTable, oos);
    }

    private void write(BrowseTable browseTable, ObjectOutputStream oos) throws IOException {
        if (browseTable == null) {
            oos.writeObject(null);
            return;
        }

        String tableName = browseTable.getTableName();
        oos.writeObject(tableName);

        TableCriteria criteria = browseTable.getCriteria();
        write(criteria, oos);

        List<BrowseTableReference> references = browseTable.getReferences();
        oos.writeObject(references.size());

        for (BrowseTableReference reference : references) {
            write(reference, oos);
        }
    }

    private void write(TableCriteria criteria, ObjectOutputStream oos) throws IOException {
        if (criteria == null) {
            oos.writeObject(null);
            return;
        }
        List<TableCriterion> criterions = criteria.getCriterions();
        oos.writeObject(criterions.size());
        for (TableCriterion criterion : criterions) {
            write(criterion, oos);
        }
    }

    private void write(TableCriterion criterion, ObjectOutputStream oos) throws IOException {
        oos.writeObject(criterion.getColumnName());
        oos.writeObject(criterion.getOp());
        oos.writeObject(criterion.getValue());
    }

    private void write(BrowseTableReference reference, ObjectOutputStream oos) throws IOException {
        oos.writeObject(reference.getSourceColumns());
        oos.writeObject(reference.getTargetColumns());
        BrowseTable targetBrowseTable = reference.getTargetBrowseTable();
        write(targetBrowseTable, oos);
    }


    @Override
    public BrowseTable deserialize(InputStream inputStream) throws Exception {
        ObjectInputStream ois = new ObjectInputStream(inputStream);
        return readBrowseTable(ois);
    }

    private BrowseTable readBrowseTable(ObjectInputStream ois) throws IOException, ClassNotFoundException {
        Object o = ois.readObject();
        if (o == null) {
            return null;
        }
        String tableName = (String) o;
        BrowseTable browseTable = new BrowseTable(tableName);
        return readBrowseTableContent(browseTable, ois);
    }

    private BrowseTable readBrowseTableContent(BrowseTable browseTable, ObjectInputStream ois) throws IOException, ClassNotFoundException {

        readCriteria(browseTable, ois);

        readReferences(browseTable, ois);

        return browseTable;
    }

    private void readReferences(BrowseTable browseTable, ObjectInputStream ois) throws IOException, ClassNotFoundException {
        Object o = ois.readObject();
        if (o == null) {
            return;
        }
        Integer referencesCount = (Integer) o;

        for (int i = 0; i < referencesCount; i++) {
            readReference(browseTable, ois);
        }
    }

    private void readReference(BrowseTable browseTable, ObjectInputStream ois) throws IOException, ClassNotFoundException {
        String[] sourceColumns = (String[]) ois.readObject();
        String[] targetColumns = (String[]) ois.readObject();
        String tableName = (String) ois.readObject();
        BrowseTable targetTable = browseTable.browse(tableName).on(sourceColumns).references(targetColumns);
        readBrowseTableContent(targetTable, ois);
    }

    private void readCriteria(BrowseTable browseTable, ObjectInputStream ois) throws IOException, ClassNotFoundException {
        Object o = ois.readObject();
        if (o == null) {
            return;
        }
        Integer criterionCount = (Integer) o;

        for (int i = 0; i < criterionCount; i++) {
            readCriterion(browseTable, ois);
        }
    }

    private void readCriterion(BrowseTable browseTable, ObjectInputStream ois) throws IOException, ClassNotFoundException {
        String columnName = (String) ois.readObject();
        String op = (String) ois.readObject();
        Object value = ois.readObject();
        BrowseTableCriteriaBuilder criteriaBuilder = browseTable.with(columnName);
        try {
            Method opMethod = getOpMethod(op);
            opMethod.invoke(criteriaBuilder, value);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

    private Method getOpMethod(String op) throws NoSuchMethodException {
        try {
            return BrowseTableCriteriaBuilder.class.getDeclaredMethod(op, Object.class);
        } catch (NoSuchMethodException e) {
            try {
                return BrowseTableCriteriaBuilder.class.getDeclaredMethod(op, Object[].class);
            } catch (NoSuchMethodException ex) {
                return BrowseTableCriteriaBuilder.class.getDeclaredMethod(op, String.class);
            }
        }
    }
}
