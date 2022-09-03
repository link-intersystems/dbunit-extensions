package com.link_intersystems.dbunit.meta;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ColumnListBuilder {

    public class ColumnElementBuilder {

        private ColumnBuilder columnBuilder = new ColumnBuilder();

        public ColumnElementBuilder setColumnName(String columnName) {
            columnBuilder.setColumnName(columnName);
            return this;
        }

        public ColumnElementBuilder setDataType(DataType dataType) {
            columnBuilder.setDataType(dataType);
            return this;
        }

        public ColumnElementBuilder setSqlTypeName(String sqlTypeName) {
            columnBuilder.setSqlTypeName(sqlTypeName);
            return this;
        }

        public ColumnElementBuilder setNullable(Column.Nullable nullable) {
            columnBuilder.setNullable(nullable);
            return this;
        }

        public ColumnElementBuilder setDefaultValue(String defaultValue) {
            columnBuilder.setDefaultValue(defaultValue);
            return this;
        }

        public ColumnElementBuilder setRemarks(String remarks) {
            columnBuilder.setRemarks(remarks);
            return this;
        }

        public ColumnElementBuilder setAutoIncrement(Column.AutoIncrement autoIncrement) {
            columnBuilder.setAutoIncrement(autoIncrement);
            return this;
        }

        public void addFromTemplate(Column templateColumn) {
            setRemarks(templateColumn.getRemarks());
            setAutoIncrement(templateColumn.getAutoIncrement());
            setDataType(templateColumn.getDataType());
            setSqlTypeName(templateColumn.getSqlTypeName());
            setColumnName(templateColumn.getColumnName());
            setNullable(templateColumn.getNullable());
            setDefaultValue(templateColumn.getDefaultValue());
            add();
        }

        public void add() {
            Column column = columnBuilder.build();
            columns.add(column);
            columnBuilder = new ColumnBuilder();
        }
    }

    private List<Column> columns = new ArrayList<>();

    public ColumnListBuilder() {
    }

    public ColumnListBuilder(Column[] columns) {
        this(Arrays.asList(columns));
    }

    public ColumnListBuilder(List<Column> columnList) {
        ColumnElementBuilder elementBuilder = addColumn();
        columnList.forEach(elementBuilder::addFromTemplate);
    }

    public ColumnElementBuilder addColumn() {
        return new ColumnElementBuilder();
    }

    public ColumnList build() {
        ColumnList columnList = new ColumnList(columns);
        columns = new ArrayList<>();
        return columnList;
    }

}
