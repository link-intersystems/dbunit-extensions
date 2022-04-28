/**
 * The beans package provides support for mapping java beans as {@link org.dbunit.dataset.IDataSet}s.
 *
 * <p>
 * Bean based data sets are easier to maintain then xml, xls or csv based data sets in case of schema changes,
 * because you can apply your IDE's refactoring tools. You can also add property getters that apply some kind
 * of domain logic or calculate default values.
 * </p>
 *
 * <h2>Creating a BeanDataSet</h2>
 * First you need to create the beans for the {@link com.link_intersystems.dbunit.dataset.beans.BeanDataSet} and put them
 * into a {@link com.link_intersystems.dbunit.dataset.beans.BeanList}. Each
 * {@link com.link_intersystems.dbunit.dataset.beans.BeanList} will represent one table.
 *
 * <pre>
 *     BeanList&lt;CustomerBean> customerBeanList = ...;
 *     BeanList&lt;OrderBean> orderBeanList = ...;
 * </pre>
 * <p>
 * Then construct a {@link com.link_intersystems.dbunit.dataset.beans.BeanDataSet} by passing it a {@link java.util.List}
 * of {@link com.link_intersystems.dbunit.dataset.beans.BeanList}.
 *
 * <pre>
 *     List&lt;BeanList&lt;?>> beanLists = new ArrayList&lt;>();
 *     beanLists.add(customerBeanList);
 *     beanLists.add(orderBeanList);
 *
 *     BeanTableMetaDataProvider beanTableMetaDataProvider = new DefaultBeanTableMetaDataProvider(CustomerBean.class, OrderBean.class);
 *     BeanDataSet beanDataSet = new BeanDataSet(beanLists, bean);
 * </pre>
 * <p>
 * The {@link com.link_intersystems.dbunit.dataset.beans.BeanDataSet} provides a convenient constructor for creating a
 * {@link com.link_intersystems.dbunit.dataset.beans.DefaultBeanTableMetaDataProvider} based on the bean classes that the
 * {@link com.link_intersystems.dbunit.dataset.beans.BeanList}s have.
 *
 * <pre>
 *     BeanDataSet beanDataSet = new BeanDataSet(beanLists);
 * </pre>
 *
 * <h2>Extending</h2>
 * The dbunit beans library depends on lis-commons-beans and can therefore be extended to other kind of beans like JPA.
 * <p>
 * The BeansFactory can then be passed to the DefaultBeanTableMetaDataProvider.
 *
 * <pre>
 *     BeansFactory beansFactory = BeansFactory.get("jpa"); // if a beans factory has been made available through META-INF/services
 *     BeanTableMetaDataProvider metaDataProvider = new DefaultBeanTableMetaDataProvider(beansFactory, classes);
 * </pre>
 *
 * <h2>Type conversion</h2>
 * Type conversion is done by the {@link com.link_intersystems.dbunit.dataset.beans.PropertyConversion}. Each
 * {@link com.link_intersystems.dbunit.dataset.beans.BeanTableMetaData} uses the
 * {@link com.link_intersystems.dbunit.dataset.beans.PropertyConversion} it gets set on construction.
 * <p>
 * When using {@link org.dbunit.dataset.IDataSet}s a {@link com.link_intersystems.dbunit.dataset.beans.BeanTableMetaData}
 * is constructed by a {@link com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider}. Usually the
 * {@link com.link_intersystems.dbunit.dataset.beans.DefaultBeanTableMetaDataProvider}. Thus you can change the
 * {@link com.link_intersystems.dbunit.dataset.beans.PropertyConversion} by passing an instance to the
 * {@link com.link_intersystems.dbunit.dataset.beans.DefaultBeanTableMetaDataProvider}'s constructor.
 *
 * <pre>
 *     BeanTableMetaDataProvider metaDataProvider = new DefaultBeanTableMetaDataProvider(propertyConversion, beanClasses);
 *     // if you also want to change the BeansFactory use
 *     BeanTableMetaDataProvider metaDataProvider = new DefaultBeanTableMetaDataProvider(beansFactory, propertyConversion, beanClasses);
 * </pre>
 * <p>
 */
package com.link_intersystems.dbunit.dataset.beans;