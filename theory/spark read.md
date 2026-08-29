If by **“Spark read”** you mean **all the common ways to read data using PySpark**, there isn't one fixed finite number because Spark supports many data sources and APIs. But for **PySpark/Data Engineering interviews**, you should know the following patterns.

# 1. Generic `format()` + `load()` — most important

This is the most flexible approach.

```python
df = spark.read \
    .format("parquet") \
    .load("/path/data")
```

For CSV:

```python
df = spark.read \
    .format("csv") \
    .load("/path/data")
```

JSON:

```python
df = spark.read \
    .format("json") \
    .load("/path/data")
```

Delta:

```python
df = spark.read \
    .format("delta") \
    .load("/path/data")
```

The general syntax is:

```python
spark.read.format("<format>").load("<path>")
```

---

# 2. `spark.read.<format>()`

Spark provides shortcut methods for several formats.

### CSV

```python
df = spark.read.csv("/path/data")
```

### JSON

```python
df = spark.read.json("/path/data")
```

### ORC

```python
df = spark.read.orc("/path/data")
```

### Parquet

```python
df = spark.read.parquet("/path/data")
```

So:

```python
spark.read.csv()
spark.read.json()
spark.read.orc()
spark.read.parquet()
```

are shortcut versions of the generic `format().load()` approach.

For example:

```python
spark.read.parquet("/data/sales")
```

is essentially:

```python
spark.read.format("parquet").load("/data/sales")
```

---

# 3. Read CSV with options

You can specify options directly.

```python
df = spark.read.csv(
    "/data/customers",
    header=True,
    inferSchema=True
)
```

Or:

```python
df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/data/customers")
```

Or when we do not want to infer schema from a file, we can create a list of columns and then give it to df as 

```python
columns = ["name", "age", "gender",....]
df = spark.read \
    .format("csv") \
    .load("/data/customers") \
    .toDF(*columns)
```

Or when we want to define not only the column names but also datatypes

```python
schema = "name string, age int, gender string"
df = spark.read \
    .format("csv") \
    .schema(schema) \
    .load("/data/customers") 
```

Common options:

```python
.option("header", "true")
.option("inferSchema", "true")
.option("delimiter", ",")
.option("sep", ",")
.option("quote", '"')
.option("escape", '"')
.option("nullValue", "NULL")
.option("multiline", "true")
.schema(schema)
```

---

# 4. Read using an explicit schema

Instead of:

```python
inferSchema=True
```

you can define the schema yourself.

```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])

df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .csv("/data/employees")
```

This is generally preferable in production pipelines because you aren't relying on Spark to infer types.

---

# 5. Read Parquet

### Method 1

```python
df = spark.read.parquet("/data/sales")
```

### Method 2

```python
df = spark.read \
    .format("parquet") \
    .load("/data/sales")
```

### Multiple paths

```python
df = spark.read.parquet(
    "/data/sales/2025",
    "/data/sales/2026"
)
```

You can also use a list:

```python
paths = [
    "/data/sales/2025",
    "/data/sales/2026"
]

df = spark.read.parquet(*paths)
```

---

# 6. Read JSON

### Simple

```python
df = spark.read.json("/data/events")
```

### Generic

```python
df = spark.read \
    .format("json") \
    .load("/data/events")
```

### Multiline JSON

```python
df = spark.read \
    .option("multiLine", "true") \
    .json("/data/events.json")
```

---

# 7. Read ORC

```python
df = spark.read.orc("/data/employees")
```

or:

```python
df = spark.read \
    .format("orc") \
    .load("/data/employees")
```

---

# 8. Read Delta

This is particularly important if you're working with Databricks.

### Path-based

```python
df = spark.read \
    .format("delta") \
    .load("/data/sales")
```

You can also use:

```python
df = spark.read.format("delta").load("s3://bucket/sales")
```

or:

```python
df = spark.read.format("delta").load("abfss://container@storage.dfs.core.windows.net/sales")
```

---

# 9. Read Delta using a table name

Instead of a path:

```python
df = spark.read.table("employees")
```

For example:

```python
df = spark.read.table("silver.employees")
```

This reads a registered table.

---

# 10. Read using SQL

You can use Spark SQL:

```python
df = spark.sql("""
    SELECT *
    FROM employees
""")
```

This is another very common way to read data.

You can also specify a path.

For example, for Parquet:

```python
df = spark.sql("""
    SELECT *
    FROM parquet.`/data/employees`
""")
```

For Delta:

```python
df = spark.sql("""
    SELECT *
    FROM delta.`/data/employees`
""")
```

For CSV:

```python
df = spark.sql("""
    SELECT *
    FROM csv.`/data/employees`
""")
```

The exact SQL data-source support can depend on the Spark environment/version.

---

# 11. Read a registered table with `table()`

You don't have to use `spark.sql()`.

```python
df = spark.table("employees")
```

or:

```python
df = spark.read.table("employees")
```

These are commonly used when the table is registered in the catalog.

---

# 12. Read JDBC databases

For databases such as:

* Oracle
* MySQL
* PostgreSQL
* SQL Server

you can use JDBC.

```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employees") \
    .option("user", username) \
    .option("password", password) \
    .load()
```

For example:

```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:1521/service") \
    .option("dbtable", "EMPLOYEES") \
    .option("user", "user") \
    .option("password", "password") \
    .load()
```

---

# 13. Read a JDBC query instead of a table

You can provide a SQL query using `dbtable`:

```python
query = """
(
    SELECT emp_id, name, salary
    FROM employees
    WHERE salary > 50000
) tmp
"""

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", query) \
    .option("user", username) \
    .option("password", password) \
    .load()
```

This is very useful for extracting only the required records from a database.

---

# 14. JDBC with `query` option

Depending on the Spark/JDBC setup, you can also use:

```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("query", "SELECT * FROM employees") \
    .option("user", username) \
    .option("password", password) \
    .load()
```

**Important:** don't use `query` together with options such as `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` in the same way you would with `dbtable`; for parallel JDBC reads, a subquery through `dbtable` is commonly used.

---

# 15. Read using `load()` alone

You can sometimes specify the format through Spark configuration/defaults:

```python
df = spark.read.load("/data/employees")
```

For example, if the default data source is Parquet:

```python
spark.read.load("/data/employees")
```

is effectively reading Parquet.

You can explicitly configure the default:

```python
spark.conf.set(
    "spark.sql.sources.default",
    "parquet"
)
```

Then:

```python
df = spark.read.load("/data/employees")
```

But in production code, explicitly specifying the format is generally clearer.

---

# 16. Read using a path with wildcard

You can use glob patterns.

```python
df = spark.read.parquet("/data/sales/2026/*.parquet")
```

For example:

```text
/data/sales/2026/
├── sales_jan.parquet
├── sales_feb.parquet
├── sales_mar.parquet
└── sales_apr.parquet
```

Spark reads matching files.

---

# 17. Read multiple directories

You can provide multiple paths:

```python
df = spark.read \
    .parquet(
        "/data/sales/2025",
        "/data/sales/2026"
    )
```

---

# 18. Read partitioned data

Suppose:

```text
sales/
├── year=2025/
│   ├── month=01/
│   └── month=02/
│
└── year=2026/
    ├── month=01/
    └── month=02/
```

You can simply do:

```python
df = spark.read.parquet("/data/sales")
```

Spark can automatically discover the partition columns:

```text
year
month
```

Then:

```python
df.printSchema()
```

might show:

```text
root
 |-- order_id: long
 |-- amount: double
 |-- year: integer
 |-- month: integer
```

And:

```python
df.filter("year = 2026")
```

can benefit from **partition pruning**.

---

# 19. Read with `recursiveFileLookup`

If files are nested in directories and you want Spark to recursively search for files:

```python
df = spark.read \
    .format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("/data/sales")
```

This is useful when directory structure isn't being used as partition discovery.

**Important:** `recursiveFileLookup` disables partition inference for the read.

---

# 20. Read using `pathGlobFilter`

For example:

```python
df = spark.read \
    .format("parquet") \
    .option("pathGlobFilter", "*.parquet") \
    .load("/data/sales")
```

You can filter which files Spark considers based on the glob pattern.

---

# 21. Read Delta using Time Travel

Very important for Delta interviews.

### Version

```python
df = spark.read \
    .format("delta") \
    .option("versionAsOf", 5) \
    .load("/data/employees")
```

### Timestamp

```python
df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2026-08-20 10:00:00") \
    .load("/data/employees")
```

---

# 22. Read Delta using SQL Time Travel

```sql
SELECT *
FROM employees VERSION AS OF 5;
```

or:

```sql
SELECT *
FROM employees TIMESTAMP AS OF '2026-08-20 10:00:00';
```

---

# 23. Read CSV from a single file

```python
df = spark.read \
    .option("header", True) \
    .csv("/data/employees.csv")
```

---

# 24. Read CSV from multiple files

```python
df = spark.read \
    .option("header", True) \
    .csv("/data/employees/")
```

If the directory contains:

```text
employees/
├── part1.csv
├── part2.csv
└── part3.csv
```

Spark reads all matching CSV files.

---

# 25. Read with `wholeTextFiles` — RDD approach

This isn't a DataFrame read, but you may encounter it when working with RDDs:

```python
rdd = sc.wholeTextFiles("/data/text/")
```

This reads each file as a key-value pair:

```text
(file_path, file_contents)
```

For example:

```python
rdd = sc.wholeTextFiles("/data/*.txt")
```

This is different from:

```python
spark.read.text("/data/")
```

because the latter returns a DataFrame.

---

# 26. Read text files into a DataFrame

```python
df = spark.read.text("/data/logs")
```

Result:

```text
+----------------------+
|value                 |
+----------------------+
|2026-08-29 INFO Start |
|2026-08-29 INFO End   |
+----------------------+
```

Generic form:

```python
df = spark.read \
    .format("text") \
    .load("/data/logs")
```

---

# 27. Read binary files

Spark also provides:

```python
df = spark.read.format("binaryFile").load("/data/images")
```

This produces columns such as:

```text
path
modificationTime
length
content
```

---

# 28. The important `spark.read` methods

For interviews, remember this group:

```python
spark.read.csv()
spark.read.json()
spark.read.parquet()
spark.read.orc()
spark.read.text()
spark.read.table()
spark.read.format().load()
```

And for database reads:

```python
spark.read.jdbc()
```

For example:

```python
df = spark.read.jdbc(
    url=jdbc_url,
    table="employees",
    properties=properties
)
```

---

# 29. `spark.read.jdbc()` specifically

Instead of:

```python
spark.read.format("jdbc") \
    .option("url", url) \
    .option("dbtable", "employees") \
    .option("user", user) \
    .option("password", password) \
    .load()
```

you can use:

```python
df = spark.read.jdbc(
    url=url,
    table="employees",
    properties={
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.driver.OracleDriver"
    }
)
```

This is a convenient JDBC API.

---

# 30. JDBC parallel read

For a large database table:

```python
df = spark.read.jdbc(
    url=url,
    table="employees",
    column="emp_id",
    lowerBound=1,
    upperBound=1000000,
    numPartitions=10,
    properties=properties
)
```

Spark can create multiple JDBC partitions and read data in parallel.

This is an important **Data Engineering interview topic**.

---

# 31. Summary — all major ways

Think of Spark reads in these categories:

```text
                    SPARK READ
                        │
       ┌────────────────┼─────────────────┐
       │                │                 │
   File formats      Tables/SQL        Databases
       │                │                 │
       ├─ CSV           ├─ table()        └─ JDBC
       ├─ JSON          ├─ read.table()
       ├─ Parquet       └─ spark.sql()
       ├─ ORC
       ├─ Text
       ├─ Binary
       └─ Delta
```

### Most important syntax patterns

```python
# 1. Generic
spark.read.format("parquet").load(path)

# 2. Shortcut
spark.read.parquet(path)

# 3. With options
spark.read.option("header", True).csv(path)

# 4. With schema
spark.read.schema(schema).csv(path)

# 5. Registered table
spark.read.table("database.table")

# 6. Spark SQL
spark.sql("SELECT * FROM database.table")

# 7. JDBC
spark.read.jdbc(url, table, properties)

# 8. Delta
spark.read.format("delta").load(path)

# 9. Delta Time Travel
spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load(path)
```

## ⭐ For a PySpark interview, memorize this hierarchy

```text
spark.read
│
├── format()
│     └── load()
│
├── csv()
├── json()
├── parquet()
├── orc()
├── text()
├── jdbc()
├── table()
│
└── SQL
      └── spark.sql()
```

The **most important distinction** is:

> `spark.read.format("X").load(path)` is the **generic DataFrameReader API**, while methods like `spark.read.csv()`, `spark.read.parquet()`, and `spark.read.json()` are **format-specific convenience methods**.

And for Databricks/Delta interviews, make sure you can explain **path-based Delta read vs table-based read vs SQL read vs Time Travel read**, because those are asked much more often than obscure file formats.
