package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.*;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class RowFilterConsumerTest {

    private IDataSet tinySakilaDataSet;
    private RowFilterConsumer rowFilterConsumer;
    private IDataSetConsumer dataSetConsumer;
    private DefaultTable actorTable;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        ITable testActorTable = tinySakilaDataSet.getTable("actor");
        DefaultTableMetaData actorMetaData = new DefaultTableMetaData("actor", testActorTable.getTableMetaData().getColumns(), new String[]{"actor_id"});
        actorTable = new DefaultTable(actorMetaData);
        actorTable.addTableRows(testActorTable);

        rowFilterConsumer = new RowFilterConsumer();
        dataSetConsumer = mock(IDataSetConsumer.class);
        rowFilterConsumer.setSubsequentConsumer(dataSetConsumer);
    }


    @Test
    void withoutRowFilterFactory() throws DataSetException {
        rowFilterConsumer.setRowFilterFactory(null);

        rowFilterConsumer.startDataSet();

        rowFilterConsumer.startTable(actorTable.getTableMetaData());
        rowFilterConsumer.row(new TableUtil(actorTable).getRowById("2").toArray());

        verify(dataSetConsumer, times(1)).row(eq(new Object[]{"2", "NICK", "WAHLBERG", "2006-02-15 04:34:33.0"}));
    }

    @Test
    void filterRow() throws DataSetException {
        rowFilterConsumer.setRowFilterFactory(t -> r -> {
            try {
                return r.getColumnValue("actor_id").equals("2");
            } catch (DataSetException e) {
                throw new RuntimeException(e);
            }
        });

        rowFilterConsumer.startDataSet();

        rowFilterConsumer.startTable(actorTable.getTableMetaData());
        rowFilterConsumer.row(new TableUtil(actorTable).getRowById("1").toArray());

        verify(dataSetConsumer, never()).row(any());
    }

    @Test
    void letRowPass() throws DataSetException {
        rowFilterConsumer.setRowFilterFactory(t -> r -> {
            try {
                return r.getColumnValue("actor_id").equals("2");
            } catch (DataSetException e) {
                throw new RuntimeException(e);
            }
        });

        rowFilterConsumer.startDataSet();

        rowFilterConsumer.startTable(actorTable.getTableMetaData());
        rowFilterConsumer.row(new TableUtil(actorTable).getRowById("2").toArray());

        verify(dataSetConsumer, times(1)).row(eq(new Object[]{"2", "NICK", "WAHLBERG", "2006-02-15 04:34:33.0"}));
    }
}