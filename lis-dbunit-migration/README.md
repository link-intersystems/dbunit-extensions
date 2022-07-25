A high level api for migrating data sets.

At the moment only flyway migrations are supported.

# Migrate a data set flat xml file to another database version using flyway 

1. Create a DataSetFlywayMigration

       DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();

2. Set the source data set file

   Since `DataSetFlywayMigration` implements `DataSetProducerSupport` you can use a lot of utility
   methods to define the source data set.

       flywayMigration.setFlatXmlProducer("somepath/flat-v1.xml");

3. Set the target data set file
   
   Since `DataSetFlywayMigration` implements `DataSetConsumerSupport` you can use a lot of utility
   methods to define the target data set.

       flywayMigration.setFlatXmlConsumer("somepath/flat-v2.xml");

4. Tell the migration command which container should be used:

       flywayMigrationCommand.setDatabaseContainerSupport(DatabaseContainerSupportFactory.forPostgres("postgres:latest"));

5. Tell the migration command on which flyway version the source data set is based.

       flywayMigration.setSourceVersion("1");

6. Optionally tell the migration command to which flyway version the data set should be migrated.

       flywayMigration.setTargetVersion("2"); // flyway uses the latest version if omitted.

7. Execute the migration

       flywayMigration.exec();


# Migrate a collection of DataSets

    DataSetCollectionFlywayMigration dataSetCollectionMigration = new DataSetCollectionFlywayMigration(sourcePath);
    
    dataSetCollectionMigration.addDefaultFilePatterns();
    dataSetCollectionMigration.setDatabaseContainerSupport(DatabaseContainerSupportFactory.forPostgres("postgres:latest"));
    dataSetCollectionMigration.setLocations("com/link_intersystems/dbunit/migration/postgres");
    dataSetCollectionMigration.setTargetPathSupplier(new BasepathTargetPathSupplier(targetPath));
    dataSetCollectionMigration.setSourceVersion("1");
   
    // optionally sort tables before inserting to ensure foreign key contraints.
    TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
    ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
    dataSetCollectionMigration.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));
    
    DataSetCollectionMigrationResult result = dataSetCollectionMigration.exec();
