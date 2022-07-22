
![Java CI with Maven](https://github.com/link-intersystems/dbunit-extensions/workflows/Java%20CI%20with%20Maven/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/link-intersystems/dbunit-extensions/badge.svg?branch=master)](https://coveralls.io/github/link-intersystems/dbunit-extensions?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.link-intersystems.dbunit/lis-dbunit)](https://mvnrepository.com/artifact/com.link-intersystems.dbunit)

# DBUnit compatibility 

The dbunit compatibility tests, test this library against dbunit versions 2.4.6 - 2.7.3.

![DBUnit Compatibility Test](https://github.com/link-intersystems/dbunit-extensions/workflows/DBUnit%20Compatibility%20Tests/badge.svg)

All modules in this project are at least compatible with the dbunit versions within the tested range. 
Limitations and exceptions are listed below.

- lis-dbunit-beans
  - Support for BigInteger bean property types does not work.
  - TypeConversionException.getCause will always be null. You must use TypeConversionException.getException.
  - slf4j api version 1.4.3 must be added.

## [lis-dbunit-migration](lis-dbunit-migration/README.md)

A module that provides support for data set migration.

    IDataSet sourceDataSet = ...; // some data set to migrate.

    DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();

    // DataSetFlywayMigration implements DataSetProducerSupport. Take a look at DataSetProducerSupport for details.
    flywayMigrationCommand.setDataSetProducer(sourceDataSet);
    
    CopyDataSetConsumer migratedDataSet = new CopyDataSetConsumer(); 
    // DataSetFlywayMigration implements DataSetConsumerSupport. Take a look at DataSetConsumerSupport for details.
    flywayMigration.setDataSetConsumer(migratedDataSet); 
    
    // set a testcontainer to use for migration
    flywayMigration.setJdbcDatabaseContainerFactory(() -> new PostgreSQLContainer<>("postgres:latest")));

    // set the flyway version of the source data set.
    flywayMigration.setSourceVersion("1");

    // tell the migration where the flyway migration files are located
    flywayMigration.setLocations("com/link_intersystems/dbunit/migration/postgres");
    
    // migrate the data set.
    flywayMigrationCommand.exec();

## [lis-dbunit-commands](lis-dbunit-commands/README.md)

A high level api for common dbunit tasks.

    Connection sourceConnection = ...; // java.sql.Connection
    DatabaseConnection databaseConnection = new DatabaseConnection(sourceConnection);
    DatabaseDataSet databaseDataSet = new DatabaseDataSet(databaseConnection, false);
    
    DataSetCommand migrateCommand = new DataSetCommand(databaseDataSet);
    migrateCommand.setTables("actor", "film_actor", "film");
    migrateCommand.setTableOrder(new DatabaseTableOrder(databaseConnection));
    migrateCommand.setResultDecorator(ds -> new ConsistentDatabaseDataSet(databaseConnection, ds));
    
    migrateCommand.setCsvConsumer("target/export/csv");

    migrateCommand.exec();


## [lis-dbunit-dataset](lis-dbunit-dataset/README.md)

For details take a look at [lis-dbunit-dataset](lis-dbunit-dataset/README.md)

### TableBrowser

The `TableBrowser` can be used to extract a dataset based on a kind of extract description.
This description can be build using a domain-specific language. E.g.

    BrowseTable actor = new BrowseTable("actor");
    actor.with("actor_id").in(1, 2, 3);
    BrowseTable filmActor = actor.browse("film_actor").natural();
    BrowseTable film = filmActor.browse("film").natural();
    film.browse("language").on("original_language_id").references("language_id")
    film.browse("inventory").natural();

    IDataSet dataSet = tableBrowser.browse(actor);

### ConsistentDataSetLoader

The `ConsistentDataSetLoader` is another option to load consistent datasets. You can
pass it an SQL-Select statement.

    IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(200));

## lis-dbunit-table

Provides extensions for dealing with dbunit tables. 

- **MergedTable**: Merged two or more tables by the primary key definition so that the result table's rows are distinct.
- **TableList**: A list of tables that can be packed to make the tables unique using MergedTables.
- **CellRowFilter**: Filters rows based on a column predicate.
- **ColumnList**: Provides filter and query methods for a list of columns.
- **ColumnPredicates**: Convenience class that provides reusable predicates that can be applied to columns.
- **TableUtil**: Convenience methods for table related queries.

        ITable table = ...;
        TableUtil tableUtil = new TableUtil(table);

        Row row = tableUtil.getRowById(321);
    
        for (Object cellValua : row) {
            System.out.println(cellValue)
        }

## lis-dbunit-beans

Provides Java Beans support for IDataSets which allows you to define a data set from a collection of beans. Since
Java Beans are based on Java classes you can use refactoring tools to reflect database schema changes.

    List<BeanList<?>> beanLists = new ArrayList<>();

    BeanList<EmployeeBean> employeeBeans = new BeanList<>(EmployeeBean.class, asList(EmployeeBean.king(), EmployeeBean.blake()));
    beanLists.add(employeeBeans);

    BeanList<DepartmentBean> departmentBeans = new BeanList<>(DepartmentBean.class, asList(DepartmentBean.accounting(), DepartmentBean.sales(), DepartmentBean.research()));
    beanLists.add(departmentBeans);

    beanDataSet = new BeanDataSet(beanLists);

The `ITableMetaData` for bean based data sets is determined using a `BeanTableMetaDataProvider`. The `DefaultBeanTableMetaDataProvider`
can be customized by setting a `PropertyConversion`. The `PropertyConversion` is responsible for converting bean properties to
database types and vice versa. The `DefaultPropertyConversion` uses the `DefaultDataTypeRegistry` and the `DefaultPropertyTypeRegistry`
to convert property values.

## lis-dbunit-sql

Provides support for generating sql insert scripts from a IDataSet.

    // Instantiate a SqlDialect 
    // Either from 
    // - com.link-intersystems.commons:lis-commons-sql 
    // or 
    // - com.link-intersystems.commons:lis-commons-sql-hibernate
    SqlDialect sqlDialect = new DefaultSqlDialect();

    // Create a SqlStatementWriter that generates the sql script.
    Writer writer = ....; // from java.io
    SqlStatementWriter sqlStatementWriter = new SqlStatementWriter(sqlDialect, writer);

    // Configure the output format if needed
    SqlFormatSettings sqlFormatSettings = new SqlFormatSettings();
    sqlStatementWriter.setSqlFormatSettings(sqlFormatSettings);
    
    IDataSet dataSet = ...; // A dbunit data set to export.
    DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(dataSet);
    dataSetProducerAdapter.setConsumer(sqlStatementWriter);
    dataSetProducerAdapter.produce();

