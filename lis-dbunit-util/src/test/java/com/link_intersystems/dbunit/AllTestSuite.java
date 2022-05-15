package com.link_intersystems.dbunit;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * The {@link AllTestSuite} ensures that tests are executed in the following order:
 *
 * <ul>
 *     <li>{@link UnitTest}<br/>Test only a single class in isolation.</li>
 *     <li>{@link ComponentTest}<br/>Test an object graph (multiple connected classes).</li>
 * </ul>
 * <p>
 * Since the object graph that a {@link ComponentTest} tests might contain a lot of
 * different classes that might also be tested in separate {@link UnitTest}s, you should first fix {@link UnitTest}s
 * before fixing {@link ComponentTest}s.
 *
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@Suite
@SelectClasses({UnitTestSuite.class, ComponentTestSuite.class})
public class AllTestSuite {

}
