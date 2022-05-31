# lis-dbunit-dataset

Provides extensions for dbunit data sets.

## Maven Dependency

    <dependency>
        <groupId>com.link-intersystems.dbunit</groupId>
        <artifactId>lis-dbunit-dataset</artifactId>
        <version>RELEASE</version>
    </dependency>

## Consistent DataSets

A consistent dataset is a dataset that contains all tables and entities that are needed 
to comply with foreign key constrains of a datasource.

lis-dbunit-dataset provides different options to achieve this.

### TableBrowser

The `TableBrowser` can be used to extract a dataset based on a kind of extract description.
This description can be build using a domain-specific language. E.g.

    BrowseTable filmActor = new BrowseTable("film_actor");
    filmActor.with("film_id").eq(200);
        
    BrowseTable actor = filmActor.browse("actor").natural();
    actor.with("first_name").like("W%");

    // browse natural means that you want the TableBrowser to select
    // a proper reference based on the foreign key meta data.
    // Outgoing references are preferred over incoming references.
    BrowseTable film = filmActor.browse("film").natural();

    // You can also tell the TableBrowser how it should browse
    // to another table by defining the join columns.
    film.browse("language").on("original_language_id").references("language_id");

    film.browse("inventory").natural();
    
After you have build a description you can either execute it with the `TableBrowser`

    IDataSet dataSet = tableBrowser.browse(actor);

or you can make it consistent before browsing
   
    Connection connection = ...; // from java.sql
    TableReferenceMetaData tableReferenceMetaData = new ConnectionMetaData(connection);

    actor.makeConsistent(tableReferenceMetaData);

    IDataSet dataSet = tableBrowser.browse(actor);


### ConsistentDataSetLoader

The `ConsistentDataSetLoader` is another option to load consistent datasets. You can
pass it an SQL-Select statement. After the SQL-Select statement is executed, the
result set is analyzed to find out with table is selected. The `ConsistentDataSetLoader`
will then resolve the meta data of that table and follow all imported foreign keys (outgoing references)
to other tables and load the referenced entities. E.g.

    IDataSet dataSet = dataSetLoader.load("SELECT * from film_actor where film_actor.film_id = ?", Integer.valueOf(200));

The result dataset will contain all referenced tables and entities:

    String[] tableNames = dataSet.getTableNames();
    assertArrayEquals(new String[]{"film_actor", "film", "actor", "language"}, tableNames);

    ITable filmActorTable = dataSet.getTable("film_actor");
    assertEquals(3, filmActorTable.getRowCount(), "film_actor entity count");
    TableUtil filmActorUtil = new TableUtil(filmActorTable);
    
    assertNotNull(filmActorUtil.getRowById(9, 200));
    assertNotNull(filmActorUtil.getRowById(102, 200));
    assertNotNull(filmActorUtil.getRowById(139, 200));
    
    ITable actorTable = dataSet.getTable("actor");
    assertEquals(3, actorTable.getRowCount(), "actor entity count");
    TableUtil actorUtil = new TableUtil(actorTable);
    assertNotNull(actorUtil.getRowById(9));
    assertNotNull(actorUtil.getRowById(102));
    assertNotNull(actorUtil.getRowById(139));
    
    ITable filmTable = dataSet.getTable("film");
    assertEquals(1, filmTable.getRowCount(), "film entity count");
    TableUtil filmUtil = new TableUtil(filmTable);
    assertNotNull(filmUtil.getRowById(200));
    
    ITable languageTable = dataSet.getTable("language");
    assertEquals(1, languageTable.getRowCount(), "language entity count");
    TableUtil languageUtil = new TableUtil(languageTable);
    assertNotNull(languageUtil.getRowById(1));
