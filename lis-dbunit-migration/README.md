A high level api for migrating data sets.

At the moment only flyway migrations are supported.

# Migrate a data set flat xml file to another database version using flyway 

1. Create a DataSetFlywayMigration

       DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();

2. Set the source data set file

   Since `DataSetFlywayMigration` implements `DataSetProducerSupport` you can use a lot of utility
   methods to define the source data set.

       flywayMigration.setFlatXmlProducer("somepath/flat-v1.xml");

5. Set the target data set file
   
   Since `DataSetFlywayMigration` implements `DataSetConsumerSupport` you can use a lot of utility
   methods to define the target data set.

       flywayMigration.setFlatXmlConsumer("somepath/flat-v2.xml");

6. Tell the migration command which container should be used:

       flywayMigration.setJdbcDatabaseContainerFactory(() -> new PostgreSQLContainer<>("postgres:latest")));

7. Tell the migration command on which flyway version the source data set is based.

       flywayMigration.setSourceVersion("1");

8. Optionally tell the migration command to which flyway version the data set should be migrated.

       flywayMigration.setTargetVersion("2"); // flyway uses the latest version if omitted.

9. Execute the migration

       flywayMigration.exec();
