# lis-dbunit-stream

This module contains a lot of utility classes for dbunit stream handline.

## commands package
The commands package contains a high level api for common dbunit tasks.

### Basic data set migration command setup

1. Create a DataSetCommand, e.g. based on a database connection:

       Connection sourceConnection = ...; // java.sql.Connection
       DatabaseConnection databaseConnection = new DatabaseConnection(sourceConnection);
       DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);
       
       DataSetCommand dataSetCommand = new DataSetCommand(databaseDataSet);

2. Filter the tables to export

       dataSetCommand.setTables("actor", "film_actor", "film");

3. Order the tables based on the foreign key constraints

       dataSetCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));

4. Decorate the result. E.g. make it consistent by following foreign keys:

       dataSetCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));


### Migrate to another database

    dataSetCommand.setDatabaseConsumer(targetDatabaseConnection, DatabaseOperation.UPDATE);
    dataSetCommand.exec();

### Migrate as CSV

    dataSetCommand.setCsvConsumer("target/export/csv");
    dataSetCommand.exec();

### Migrate as flat XML

    dataSetCommand.setFlatXmlConsumer("target/export/flat.xml");
    dataSetCommand.exec();

### Migrate as XML

    dataSetCommand.setXmlConsumer("target/export/flat.xml");
    dataSetCommand.exec();

### Transform one data set format to another

E.g. convert a flat xml format to csv.

    InputStream in = ...;
    FlatXmlDataSet flatXmlDataSet = new FlatXmlDataSetBuilder().build(in);

    DataSetCommand dataSetCommand = new DataSetCommand(flatXmlDataSet);     
    dataSetCommand.setCsvConsumer("target/csv");

    dataSetCommand.exec();