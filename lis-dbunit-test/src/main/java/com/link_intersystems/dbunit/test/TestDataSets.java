package com.link_intersystems.dbunit.test;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestDataSets {

    public static IDataSet getTinySakilaDataSet() throws IOException, DataSetException {
        InputStream in = TestDataSets.getTinySakilaResource().openStream();
        FlatXmlDataSet flatXmlDataSet = getFlatXmlDataSet(in);
        ReplacementDataSet replacementDataSet = new ReplacementDataSet(flatXmlDataSet);
        replacementDataSet.addReplacementObject("[null]", ITable.NO_VALUE);
        replacementDataSet.setStrictReplacement(true);
        return replacementDataSet;
    }

    private static FlatXmlDataSet getFlatXmlDataSet(InputStream in) throws DataSetException {
        FlatXmlDataSet flatXmlDataSet;
        try {
            Constructor<FlatXmlDataSet> oldApiConstructor = FlatXmlDataSet.class.getDeclaredConstructor(InputSource.class);
            flatXmlDataSet = oldApiConstructor.newInstance(new InputSource(in));
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            flatXmlDataSet = new FlatXmlDataSetBuilder().build(in);
        }
        return flatXmlDataSet;
    }

    private static URL getTinySakilaResource() {
        return TestDataSets.class.getResource("flat.xml");
    }
}
