A high level api for common dbunit tasks.

# Basic export command setup

1. Create a DataSetExportCommand based on a database connection:

       Connection sourceConnection = ...; // java.sql.Connection
       DatabaseConnection databaseConnection = new DatabaseConnection(sourceConnection);
       DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);
    
       DataSetExportCommand dbUnitExportCommand = new DataSetExportCommand(databaseDataSet);

2. Filter the tables to export

       dbUnitExportCommand.setTables("actor", "film_actor", "film");

3. Order the tables based on the foreign key constraints

       dbUnitExportCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));

4. Decorate the result. E.g. make it consistent by following foreign keys:

       dbUnitExportCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));


## Export to another database

    Connection targetConnection = ...; // java.sql.Connection
    DatabaseConnection targetDatabaseConnection = new DatabaseConnection(targetConnection);

## Export to another database

    dbUnitExportCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.UPDATE);
    dbUnitExportCommand.exec();

## Export as CSV

    dbUnitExportCommand.setCsvConsumer("target/export/csv");
    dbUnitExportCommand.exec();

## Export as flat XML

    dbUnitExportCommand.setFlatXmlConsumer("target/export/flat.xml");
    dbUnitExportCommand.exec();

## Export as XML

    dbUnitExportCommand.setXmlConsumer("target/export/flat.xml");
    dbUnitExportCommand.exec();