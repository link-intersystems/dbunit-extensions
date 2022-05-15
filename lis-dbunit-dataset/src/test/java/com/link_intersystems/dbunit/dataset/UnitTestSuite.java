package com.link_intersystems.dbunit.dataset;

import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@Suite
@SelectPackages("com.link_intersystems.dbunit")
@IncludeTags({"UnitTest"})
public class UnitTestSuite {

}
