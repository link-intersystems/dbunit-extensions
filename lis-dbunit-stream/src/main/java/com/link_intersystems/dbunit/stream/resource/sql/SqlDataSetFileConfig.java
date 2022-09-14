package com.link_intersystems.dbunit.stream.resource.sql;

import com.link_intersystems.dbunit.database.DatabaseConnectionBorrower;
import com.link_intersystems.dbunit.stream.consumer.sql.DefaultTableLiteralFormatResolver;
import com.link_intersystems.dbunit.stream.consumer.sql.TableLiteralFormatResolver;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
import com.link_intersystems.util.config.properties.ConfigProperty;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface SqlDataSetFileConfig {

    public static final ConfigProperty<DatabaseDataSetProducerConfig> DATABASE_DATA_SET_PRODUCER_CONFIG = ConfigProperty.named("databaseDataSetProducerConfig").typed(DatabaseDataSetProducerConfig.class);
    public static final ConfigProperty<DatabaseConnectionBorrower> DATABASE_CONNECTION_BORROWER = ConfigProperty.named("databaseConnectionBorrower").typed(DatabaseConnectionBorrower.class);
    public static final ConfigProperty<TableLiteralFormatResolver> TABLE_LITERAL_FORMAT_RESOLVER = ConfigProperty.named("tableLiteralFormatResolver").typed(TableLiteralFormatResolver.class).withDefaultValue(new DefaultTableLiteralFormatResolver());

    public DatabaseDataSetProducerConfig getDatabaseDataSetProducerConfig();

    public TableLiteralFormatResolver getTableLiteralFormatResolver();

    DatabaseConnectionBorrower getDatabaseConnectionBorrower();
}
