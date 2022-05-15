
![Java CI with Maven](https://github.com/link-intersystems/dbunit-extensions/workflows/Java%20CI%20with%20Maven/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/link-intersystems/dbunit-extensions/badge.svg?branch=master)](https://coveralls.io/github/link-intersystems/dbunit-extensions?branch=master)


# DBUnit compatibility 

The dbunit compatibility tests, test this library against dbunit versions 2.4.6 - 2.7.3.

![DBUnit Compatibility Test](https://github.com/link-intersystems/dbunit-extensions/workflows/DBUnit%20Compatibility%20Tests/badge.svg)

All modules in this project are at least compatible with the dbunit versions within the tested range. 
Limitations and exceptions are listed below.

## lis-dbunit-beans

lis-dbunit-beans needs at least dbunit version 2.2, 
but not all features will properly work.

- Support for BigInteger bean property types does not work.
- TypeConversionException.getCause will always be null. You must use TypeConversionException.getException.
- slf4j api version 1.4.3 must be added.

## lis-dbunit-table

Provides extensions for dealing with dbunit tables. 

    ITable table = ...;
    TableUtil tableUtil = new TableUtil(table);

    Row row = tableUtil.getRowById(321);
    
    for (Object cellValua : row) {
        System.out.println(cellValue)
    }

## lis-dbunit-dataset

Provides extensions for dbunit data sets. E.g. if you want to load a consistent data
set that contains all tables that are referenced by foreign keys.

Here is an example based on the sakila sample database provided by mysql.

    Connection sakilaConnection = ...;
    IDatabaseConnection databaseConnection = new DatabaseConnection(sakilaConnection);
    ConsistentDataSetLoader dataSetLoader = new ConsistentDataSetLoader(databaseConnection);

    IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(200));

    String[] tableNames = dataSet.getTableNames();
    assertArrayEquals(new String[]{"film_actor", "film", "actor", "language"}, tableNames);
    
    ITable filmActorTable = dataSet.getTable("film_actor");
    assertEquals(3, filmActorTable.getRowCount(), "film_actor entity count");
    TableUtil filmActorTableQueries = new TableUtil(filmActorTable);
    
    assertNotNull(filmActorTableQueries.getRowById(9, 200));
    assertNotNull(filmActorTableQueries.getRowById(102, 200));
    assertNotNull(filmActorTableQueries.getRowById(139, 200));
    
    ITable actorTable = dataSet.getTable("actor");
    assertEquals(3, actorTable.getRowCount(), "actor entity count");
    TableUtil actorTableQueries = new TableUtil(actorTable);
    assertNotNull(actorTableQueries.getRowById(9));
    assertNotNull(actorTableQueries.getRowById(102));
    assertNotNull(actorTableQueries.getRowById(139));
    
    ITable filmTable = dataSet.getTable("film");
    assertEquals(1, filmTable.getRowCount(), "film entity count");
    TableUtil filmTableQueries = new TableUtil(filmTable);
    assertNotNull(filmTableQueries.getRowById(200));
    
    ITable languageTable = dataSet.getTable("language");
    assertEquals(1, languageTable.getRowCount(), "language entity count");
    TableUtil languageTableQueries = new TableUtil(languageTable);
    assertNotNull(languageTableQueries.getRowById(1));



