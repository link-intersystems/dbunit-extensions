/**
 * The beans.java package provides support for mapping plain old java beans to {@link org.dbunit.dataset.IDataSet}s.
 *
 * <p>
 *     Java beans are usually connected through object references and do not have any meta data that can tell how
 *     foreign keys are represented. Thus when you map pure java beans as an {@link org.dbunit.dataset.IDataSet} you
 *     have to provide beans that represent the table structure. E.g.
 *
 *     <code>
 *         public class EmployeeBean {
 *
 *              private int departmentNumber;
 *
 *              public int getDepartmentNumber(){
 *                  return departmentNumber;
 *              }
 *
 *         }
 *     </code>
 * </p>
 */
package com.link_intersystems.dbunit.dataset.beans.java;