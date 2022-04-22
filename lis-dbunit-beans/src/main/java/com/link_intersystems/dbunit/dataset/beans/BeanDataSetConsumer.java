package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.BeanInstantiationException;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.ArrayList;
import java.util.List;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanDataSetConsumer implements IDataSetConsumer {


    private static class DataSetConsumeContext {
        private List<BeanList<?>> dataSet = new ArrayList<>();

        public void add(TableConsumeContext tableConsumeContext) {
            BeanClass<?> beanClass = tableConsumeContext.beanTableMetaData.getBeanClass();
            Class<?> type = beanClass.getType();
            BeanList<?> eBeanList = new BeanList<>(type, tableConsumeContext.beans);
            dataSet.add(eBeanList);

        }
    }

    private static class TableConsumeContext {

        private List<Object> beans = new ArrayList<>();

        private IBeanTableMetaData beanTableMetaData;
        private ITableMetaData tableMetaData;

        public TableConsumeContext(IBeanTableMetaData beanTableMetaData, ITableMetaData tableMetaData) {
            this.beanTableMetaData = beanTableMetaData;
            this.tableMetaData = tableMetaData;
        }

        public void addRow(Object[] objects) throws DataSetException {
            BeanClass<?> beanClass = beanTableMetaData.getBeanClass();
            try {
                Object bean = beanClass.newInstance();
                Column[] columns = tableMetaData.getColumns();
                for (int i = 0; i < columns.length; i++) {
                    Column column = columns[i];
                    Object value = objects[i];
                    beanTableMetaData.setValue(bean, column, value);
                }

                beans.add(bean);
            } catch (BeanInstantiationException e) {
                throw new DataSetException(e);
            }
        }
    }

    private BeanTableMetaDataProvider beanTableMetaDataProvider;

    private DataSetConsumeContext dataSetConsumeContext;
    private TableConsumeContext tableConsumeContext;
    private List<BeanList<?>> beanDataSet;

    public BeanDataSetConsumer(BeanTableMetaDataProvider beanTableMetaDataProvider) {
        this.beanTableMetaDataProvider = beanTableMetaDataProvider;
    }

    @Override
    public void startDataSet() {
        dataSetConsumeContext = new DataSetConsumeContext();
    }

    @Override
    public void endDataSet() {
        beanDataSet = dataSetConsumeContext.dataSet;
        dataSetConsumeContext = null;
    }

    @Override
    public void startTable(ITableMetaData tableMetaData) throws DataSetException {
        String tableName = tableMetaData.getTableName();
        IBeanTableMetaData beanTableMetaData = beanTableMetaDataProvider.getMetaData(tableName);
        if (beanTableMetaData == null) {
            String msg = format("Can not resolve ''{0}'' to process table ''{1}''.", IBeanTableMetaData.class.getSimpleName(), tableName);
            throw new DataSetException(msg);
        }
        tableConsumeContext = new TableConsumeContext(beanTableMetaData, tableMetaData);
    }

    @Override
    public void endTable() {
        dataSetConsumeContext.add(tableConsumeContext);
        tableConsumeContext = null;
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        tableConsumeContext.addRow(objects);
    }

    public List<BeanList<?>> getBeanDataSet() {
        return beanDataSet;
    }
}
