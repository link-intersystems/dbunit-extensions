package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.beans.BeansFactory;
import com.link_intersystems.dbunit.dataset.beans.fixtures.TestBean;
import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBeanFixture;
import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBean;
import com.link_intersystems.test.ComponentTest;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
class BeanDataSetConsumerTest {

    private BeanDataSetConsumer beanDataSetConsumer;
    private BeanTableMetaDataProvider beanMetaDataProvider;
    private EmployeeBeanFixture employeeBeanFixture;

    @BeforeEach
    void setUp() throws BeanClassException {
        employeeBeanFixture = new EmployeeBeanFixture();
        beanMetaDataProvider = employeeBeanFixture.createBeanMetaDataProvider();
        beanDataSetConsumer = new BeanDataSetConsumer(this.beanMetaDataProvider);
    }

    @Test
    void consumeDataSet() throws Exception {
        beanDataSetConsumer.startDataSet();
        beanDataSetConsumer.startTable(beanMetaDataProvider.getMetaData(EmployeeBean.class));

        beanDataSetConsumer.row(employeeBeanFixture.toRow(EmployeeBean.jones()));
        beanDataSetConsumer.row(employeeBeanFixture.toRow(EmployeeBean.blake()));
        beanDataSetConsumer.endTable();
        beanDataSetConsumer.endDataSet();

        List<BeanList<?>> beanLists = beanDataSetConsumer.getBeanDataSet();
        assertEquals(1, beanLists.size());

        BeanList<?> employeeBeans = beanLists.get(0);
        assertEquals(2, employeeBeans.size());

        Object jones = employeeBeans.get(0);
        assertEquals(EmployeeBean.jones(), jones);
    }

    @Test
    void beanInstantiationException() throws Exception {
        class NoBean {
            private NoBean() {
            }
        }

        beanMetaDataProvider = new DefaultBeanTableMetaDataProvider(BeansFactory.getDefault(), NoBean.class);
        beanDataSetConsumer = new BeanDataSetConsumer(beanMetaDataProvider);

        beanDataSetConsumer.startDataSet();
        beanDataSetConsumer.startTable(beanMetaDataProvider.getMetaData(NoBean.class));

        assertThrows(DataSetException.class, () -> beanDataSetConsumer.row(employeeBeanFixture.toRow(EmployeeBean.jones())));
    }

    @Test
    void beanTableMetaDataCanNotBeResolved() {
        beanMetaDataProvider = new DefaultBeanTableMetaDataProvider(BeansFactory.getDefault(), TestBean.class);

        beanDataSetConsumer.startDataSet();

        assertThrows(DataSetException.class, () -> beanDataSetConsumer.startTable(beanMetaDataProvider.getMetaData(TestBean.class)));
    }


}