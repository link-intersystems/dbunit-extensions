A high level api for common dbunit tasks.

# Basic data set migration command setup

1. Create a DataSetMigrationCommand, e.g. based on a database connection:

       Connection sourceConnection = ...; // java.sql.Connection
       DatabaseConnection databaseConnection = new DatabaseConnection(sourceConnection);
       DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);
       
       DataSetMigrationCommand migrateCommand = new DataSetMigrationCommand(databaseDataSet);

2. Filter the tables to export

       migrateCommand.setTables("actor", "film_actor", "film");

3. Order the tables based on the foreign key constraints

       migrateCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));

4. Decorate the result. E.g. make it consistent by following foreign keys:

       migrateCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));


## Migrate to another database

    migrateCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.UPDATE);
    migrateCommand.exec();

## Migrate as CSV

    migrateCommand.setCsvConsumer("target/export/csv");
    migrateCommand.exec();

## Migrate as flat XML

    migrateCommand.setFlatXmlConsumer("target/export/flat.xml");
    migrateCommand.exec();

## Migrate as XML

    migrateCommand.setXmlConsumer("target/export/flat.xml");
    migrateCommand.exec();

# Transform one data set format to another

E.g. convert a flat xml format to csv.

    InputStream in = ...;
    FlatXmlDataSet flatXmlDataSet = new FlatXmlDataSetBuilder().build(in);

    DataSetMigrationCommand migrateCommand = new DataSetMigrationCommand(flatXmlDataSet);     
    migrateCommand.setCsvConsumer("target/csv");

    migrateCommand.exec();