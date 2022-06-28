package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestDataSets {

    public static IDataSet getTinySakilaDataSet() throws IOException, DataSetException {
        InputStream in = TestDataSets.getTinySakilaResource().openStream();
        FlatXmlDataSet flatXmlDataSet = new FlatXmlDataSetBuilder().build(in);
        ReplacementDataSet replacementDataSet = new ReplacementDataSet(flatXmlDataSet);
        replacementDataSet.addReplacementObject("[null]", ITable.NO_VALUE);
        replacementDataSet.setStrictReplacement(true);
        return replacementDataSet;
    }

    private static URL getTinySakilaResource() {
        return TestDataSets.class.getResource("flat.xml");
    }
}
