/**
 * The beans package provides support for mapping java beans as {@link org.dbunit.dataset.IDataSet}s.
 *
 *<p>
 *     Bean based data sets are easier to maintain then xml, xls or csv based data sets in case of schema changes,
 *     because you can apply your IDE's refactoring tools. You can also add property getters that apply some kind
 *     of domain logic or calculate default values.
 *</p>
 *
 * <h2>Extending</h2>
 * The dbunit beans library depends on lis-commons-beans and can therefore be extended to other kind of beans like JPA.
 *
 * The BeansFactory can then be passed to the DefaultBeanTableMetaDataProvider.
 *
 * <pre>
 *     BeansFactory beansFactory = BeansFactory.get("jpa"); // if a beans factory has been made available through META-INF/services
 *     BeanTableMetaDataProvider metaDataProvides = new DefaultBeanTableMetaDataProvider(beansFactory, classes);
 * </pre>
 *
 * <h2>Type conversion</h2>
 * To convert bean property types to {@link org.dbunit.dataset.Column} data types one can set a
 * {@link com.link_intersystems.dbunit.dataset.beans.ValueConverterRegistry} to the {@link com.link_intersystems.dbunit.dataset.beans.BeanDataSet}
 */
package com.link_intersystems.dbunit.dataset.beans;