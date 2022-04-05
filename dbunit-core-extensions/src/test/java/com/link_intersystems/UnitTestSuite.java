package com.link_intersystems;

import org.junit.platform.suite.api.ExcludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeTags;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
@Suite
@SelectPackages("com.link_intersystems")
@IncludeTags({"UnitTest"})
public class UnitTestSuite {

}
