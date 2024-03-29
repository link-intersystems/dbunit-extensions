package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.meta.TableMetaDataBuilder;
import com.link_intersystems.dbunit.stream.consumer.support.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.stream.producer.support.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * A memory saving streaming {@link IDataSetConsumer} that temporarily save the tables to a temp directory, loads them
 * again in the desired order and passed them to the subsequent {@link IDataSetConsumer}.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExternalSortTableConsumer extends DefaultChainableDataSetConsumer implements Closeable {

    private Logger logger = LoggerFactory.getLogger(ExternalSortTableConsumer.class);

    private final TableOrder tableOrder;
    private IDataSetConsumer tempDataSetConsumer;
    private File tempDir;

    private Map<String, ITableMetaData> sourceTableMetaData;

    public ExternalSortTableConsumer(TableOrder tableOrder) {
        this.tableOrder = Objects.requireNonNull(tableOrder);
    }

    @Override
    public void startDataSet() throws DataSetException {
        sourceTableMetaData = new HashMap<>();

        try {
            tempDir = createTempDirectory();
            tempDir.deleteOnExit();
            DefaultDataSetConsumerSupport defaultDataSetConsumerSupport = new DefaultDataSetConsumerSupport();
            defaultDataSetConsumerSupport.setCsvConsumer(tempDir);
            tempDataSetConsumer = defaultDataSetConsumerSupport.getDataSetConsumer();

        } catch (IOException e) {
            throw new DataSetException(e);
        }


        tempDataSetConsumer.startDataSet();
    }

    @Override
    public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
        TableMetaDataBuilder tableMetaDataBuilder = new TableMetaDataBuilder(iTableMetaData);
        sourceTableMetaData.put(iTableMetaData.getTableName(), tableMetaDataBuilder.build());
        tempDataSetConsumer.startTable(iTableMetaData);
    }

    @Override
    public void row(Object[] objects) throws DataSetException {
        tempDataSetConsumer.row(objects);
    }

    @Override
    public void endTable() throws DataSetException {
        tempDataSetConsumer.endTable();
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            tempDataSetConsumer.endDataSet();
            tempDataSetConsumer = null;

            List<String> tableNames = readTableNames();
            String[] orderedTables = tableOrder.orderTables(tableNames.toArray(new String[0]));
            writeTableNames(orderedTables);

            DefaultDataSetProducerSupport defaultDataSetProducerSupport = new DefaultDataSetProducerSupport();
            defaultDataSetProducerSupport.setCsvProducer(tempDir);
            IDataSetProducer dataSetProducer = defaultDataSetProducerSupport.getDataSetProducer();

            TableMetaDataReplacementConsumer tableMetaDataReplacementConsumer = new TableMetaDataReplacementConsumer(sourceTableMetaData::get);
            EnsureDataTypeConsumer ensureDataTypeConsumer = new EnsureDataTypeConsumer();
            ensureDataTypeConsumer.setSubsequentConsumer(getDelegate());
            tableMetaDataReplacementConsumer.setSubsequentConsumer(ensureDataTypeConsumer);
            dataSetProducer.setConsumer(tableMetaDataReplacementConsumer);
            dataSetProducer.produce();
        } finally {
            sourceTableMetaData = null;
            try {
                close();
            } catch (IOException e) {
                logger.error("External sort finished, but unable to clean up temporary directory.", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!deleteDirectory(tempDir)) {
            throw new IOException("Unable to delete temporary directory: " + tempDir);
        }
    }

    protected File createTempDirectory() throws IOException {
        return Files.createTempDirectory("SortTableConsumer").toFile();
    }

    protected boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private void writeTableNames(String[] tableNames) throws DataSetException {
        File file = new File(tempDir, "table-ordering.txt");
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            for (String tableName : tableNames) {
                printWriter.println(tableName);
            }
        } catch (IOException e) {
            throw new DataSetException(e);
        }
    }

    private List<String> readTableNames() throws DataSetException {
        File file = new File(tempDir, "table-ordering.txt");
        List<String> tableNames = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
            String tableName;
            while ((tableName = bufferedReader.readLine()) != null) {
                if (!tableName.isEmpty()) {
                    tableNames.add(tableName);
                }
            }
        } catch (IOException e) {
            throw new DataSetException(e);
        }

        return tableNames;
    }
}
