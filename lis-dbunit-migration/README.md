The lis-dbunit-migration contain modules that support automatic data set migration using plugins for the
database provider (at the moment testcontainers) and for the migration implementor (at the moment flyway).

To use the data set migration you need 3 dependencies.

      <properties>
         <!-- set to the release version you want to use -->
         <lis-dbunit-migration.version>RELEASE</lis-dbunit-migration.version>
      </properties>

      <dependency>
         <groupId>com.link-intersystems.dbunit</groupId>
         <artifactId>lis-dbunit-migration</artifactId>
         <version>${lis-dbunit-migration.version}</version>
      </dependency>
      <dependency>
         <groupId>com.link-intersystems.dbunit</groupId>
         <artifactId>lis-dbunit-migration-flyway</artifactId>
         <version>${lis-dbunit-migration.version}</version>
      </dependency>
      <dependency>
         <groupId>com.link-intersystems.dbunit</groupId>
         <artifactId>lis-dbunit-migration-testcontainers</artifactId>
         <version>${lis-dbunit-migration.version}</version>
      </dependency>

Note: Before version 1.0.5 you only need the 

      <dependency>
         <groupId>com.link-intersystems.dbunit</groupId>
         <artifactId>lis-dbunit-migration</artifactId>
         <version>${lis-dbunit-migration.version}</version>
      </dependency>

because the project was restructured in 1.0.5.


# Migrate a data set flat xml file to another database version using flyway 

1. Create a DataSetMigration

       DataSetMigration dataSetMigration = new DataSetMigration();

2. Set the source data set file

   Since `DataSetMigration` implements `DataSetProducerSupport` you can use a lot of utility
   methods to define the source data set.

       dataSetMigration.setFlatXmlProducer("somepath/flat-v1.xml");

3. Set the target data set file
   
   Since `DataSetMigration` implements `DataSetConsumerSupport` you can use a lot of utility
   methods to define the target data set.

       dataSetMigration.setFlatXmlConsumer("somepath/flat-v2.xml");

4. Tell the migration command which container should be used:

       dataSetMigration.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory("postgres:latest"));

5. Tell the migration command which `DatabaseMigrationSupport` it should use, e.g. flyway.

       FlywayMigrationConfig flywayMigrationConfig =  new FlywayMigrationConfig();
       flywayMigrationConfig.setSourceVersion("1");
       FlywayDatabaseMigrationSupport flywayMigrationSupport = new FlywayDatabaseMigrationSupport(flywayMigrationConfig);
       dataSetMigration.setDatabaseMigrationSupport(flywayMigrationSupport);

7. Optionally tell the migration support to what the target version should be. This is the version the data sets will be migrated to.

       flywayMigrationConfig.setTargetVersion("2"); // flyway uses the latest version if omitted.

8. Execute the migration

       dataSetMigration.exec();


# Migrate a collection of DataSets

     DataSetsMigrations dataSetsMigrations = new DataSetsMigrations();
     
     BasepathTargetPathSupplier basepathTargetPathSupplier = new BasepathTargetPathSupplier(sourcePath, targetPath);
     dataSetsMigrations.setTargetDataSetResourceSupplier(basepathTargetPathSupplier);
     
     fileLocationsScanner = new DataSetFileLocationsScanner(sourcePath);
     dataSetsMigrations.setDataSetResourcesSupplier(new DefaultDataSetResourcesSupplier(fileLocationsScanner, new DataSetFileDetection()));


     dataSetsMigrations.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory("postgres:latest"));

     FlywayMigrationConfig migrationConfig = new FlywayMigrationConfig();
     // set the migration config properties
     dataSetsMigrations.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

     TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
     ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
     dataSetsMigrations.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));

     MigrationsResult result = dataSetsMigrations.exec();
