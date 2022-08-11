package com.link_intersystems.dbunit.stream.producer;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.common.handlers.IllegalInputCharacterException;
import org.dbunit.dataset.common.handlers.PipelineException;
import org.dbunit.dataset.csv.CsvDataSetWriter;
import org.dbunit.dataset.csv.CsvParserException;
import org.dbunit.dataset.csv.CsvParserImpl;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.dbunit.dataset.csv.CsvDataSet.TABLE_ORDERING_FILE;

/**
 * The {@link URLCsvProducer} can be used to read csv data sets from a zip file by passing it a jar file URL,
 * e.g.<code>jar:file://target/classes/tiny-sakila-csv.zip!/</code> - mind the <code>!/</code> at the end of the URL. If
 * you just pass a url that ends with <code>.zip</code> it will be automatically converted to a jar file url, e.g.
 * <ul>
 *     <li>file://target/classes/tiny-sakila-csv.zip -> jar:file://target/classes/tiny-sakila-csv.zip!/</li>
 *     <li>http://somehost.com/tiny-sakila-csv.zip -> jar:http://somehost.com/tiny-sakila-csv.zip!/</li>
 * </ul>
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class URLCsvProducer implements IDataSetProducer {

    protected static interface TableRowConsumer {

        public void consume(Object[] row) throws DataSetException;
    }

    private static final Logger logger = LoggerFactory.getLogger(URLCsvProducer.class);

    private IDataSetConsumer consumer = new DefaultConsumer();
    private URL csvResourceURL;

    public URLCsvProducer(URL csvResourceURL) {
        this.csvResourceURL = Objects.requireNonNull(csvResourceURL);
    }

    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        logger.debug("setConsumer(consumer) - start");

        this.consumer = consumer;
    }

    public void produce() throws DataSetException {
        logger.debug("produce() - start");

        try {
            URL validZipUrl = autocorrectZipUrl(csvResourceURL);
            List<String> tableNames = getTables(validZipUrl);

            produceTables(validZipUrl, tableNames);
        } catch (IOException e) {
            throw new DataSetException("Unable to read tables from " + csvResourceURL, e);
        }
    }

    protected URL autocorrectZipUrl(URL csvResourceURL) throws MalformedURLException {
        if (csvResourceURL.getPath().endsWith(".zip") && !csvResourceURL.toString().startsWith("jar:")) {
            csvResourceURL = new URL("jar:" + csvResourceURL + "!/");
        }
        return csvResourceURL;
    }

    private void produceTables(URL csvResourceURL, List<String> tableNames) throws DataSetException {
        consumer.startDataSet();
        try {
            for (String tableName : tableNames) {
                URL tableResourceUrl = new URL(csvResourceURL, tableName + ".csv");
                produceTable(tableResourceUrl, tableName);
            }
            consumer.endDataSet();
        } catch (IOException e) {
            throw new DataSetException("error getting list of tables", e);
        }
    }

    private void produceTable(URL tableResourceUrl, String tableName) throws MalformedURLException, DataSetException {
        try {
            tryProduceTable(tableResourceUrl, tableName);
        } catch (CsvParserException | DataSetException e) {
            throw new DataSetException("error producing dataset for table '" + tableName + "'", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void tryProduceTable(URL tableResourceUrl, String tableName) throws DataSetException, CsvParserException {
        logger.debug("produceTableFile({}) - start", tableResourceUrl);

        CsvParserImpl parser = new CsvParserImpl();

        try (Reader reader = openReader(tableResourceUrl)) {
            List<List<String>> csvContent = parser.parse(reader, tableResourceUrl.toString());

            List<String> csvHeaders = csvContent.get(0);
            ITableMetaData metaData = createTableMetaData(tableName, csvHeaders);

            consumer.startTable(metaData);

            List<List<String>> csvData = csvContent.subList(1, csvContent.size());

            produceCsvData(csvData, consumer::row);

            consumer.endTable();
        } catch (PipelineException | IllegalInputCharacterException | IOException e) {
            throw new DataSetException(e);
        }
    }

    protected void produceCsvData(List<List<String>> csvData, TableRowConsumer tableRowConsumer) throws DataSetException {
        for (List<String> csvDataRow : csvData) {
            for (int col = 0; col < csvDataRow.size(); col++) {
                String colValue = csvDataRow.get(col);
                if (CsvDataSetWriter.NULL.equals(colValue)) {
                    csvDataRow.set(col, null);
                }
            }

            produceTableRow(tableRowConsumer, csvDataRow);
        }
    }

    protected void produceTableRow(TableRowConsumer tableRowConsumer, List<String> csvDataRow) throws DataSetException {
        Object[] row = csvDataRow.toArray();
        tableRowConsumer.consume(row);
    }

    protected ITableMetaData createTableMetaData(String tableName, List<String> csvHeaders) {
        Column[] columns = new Column[csvHeaders.size()];

        for (int i = 0; i < csvHeaders.size(); i++) {
            String columnName = csvHeaders.get(i);
            columnName = columnName.trim();
            columns[i] = new Column(columnName, DataType.UNKNOWN);
        }

        return new DefaultTableMetaData(tableName, columns);
    }

    /**
     * Get a list of tables that this producer will create
     *
     * @return a list of Strings, where each item is a CSV file relative to the base URL
     * @throws IOException when IO on the base URL has issues.
     */
    protected List<String> getTables(URL base) throws IOException {
        logger.debug("getTables(base={}, tableList={}) - start", base, TABLE_ORDERING_FILE);

        List<String> orderedTableNames = new ArrayList<>();

        URL tableOrderingResource = new URL(base, TABLE_ORDERING_FILE);
        try (BufferedReader reader = openReader(tableOrderingResource)) {
            String line;

            while ((line = reader.readLine()) != null) {
                String tableName = line.trim();
                if (tableName.length() > 0) {
                    orderedTableNames.add(tableName);
                }
            }
        }

        return orderedTableNames;
    }

    protected BufferedReader openReader(URL url) throws IOException {
        URLConnection urlConnection = url.openConnection();
        // Need to turn off caches so that a jar url connection will not keep open file handles.
        urlConnection.setUseCaches(false);
        return new BufferedReader(new InputStreamReader(urlConnection.getInputStream()));
    }
}
