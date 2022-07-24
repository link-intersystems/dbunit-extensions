package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A memory saving streaming {@link IDataSetConsumer} that temporarily save the tables to a temp directory, loads them
 * again in the desired order and passed them to the subsequent {@link IDataSetConsumer}.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExternalSortTableConsumer extends AbstractDataSetConsumerDelegate implements Closeable {

    private Logger logger = LoggerFactory.getLogger(ExternalSortTableConsumer.class);

    private final IDataSetConsumer subsequentConsumer;
    private final TableOrder tableOrder;
    private IDataSetConsumer tempDataSetConsumer;
    private File tempDir;

    public ExternalSortTableConsumer(IDataSetConsumer subsequentConsumer, TableOrder tableOrder) {
        this.subsequentConsumer = Objects.requireNonNull(subsequentConsumer);
        this.tableOrder = Objects.requireNonNull(tableOrder);
    }

    @Override
    public void startDataSet() throws DataSetException {
        try {
            tempDir = createTempDirectory();
            tempDir.deleteOnExit();
            DefaultDataSetConsumerSupport defaultDataSetConsumerSupport = new DefaultDataSetConsumerSupport();
            defaultDataSetConsumerSupport.setCsvConsumer(tempDir);
            tempDataSetConsumer = defaultDataSetConsumerSupport.getDataSetConsumer();
        } catch (IOException e) {
            throw new DataSetException(e);
        }


        super.startDataSet();
    }

    @Override
    protected IDataSetConsumer getDelegate() {
        return tempDataSetConsumer;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            super.endDataSet();

            List<String> tableNames = readTableNames();
            String[] orderedTables = tableOrder.orderTables(tableNames.toArray(new String[0]));
            writeTableNames(orderedTables);

            DefaultDataSetProducerSupport defaultDataSetProducerSupport = new DefaultDataSetProducerSupport();
            defaultDataSetProducerSupport.setCsvProducer(tempDir);
            IDataSetProducer dataSetProducer = defaultDataSetProducerSupport.getDataSetProducer();

            dataSetProducer.setConsumer(subsequentConsumer);
            dataSetProducer.produce();
        } finally {
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
