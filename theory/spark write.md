Yes. For **PySpark interviews**, it is useful to learn `DataFrameWriter` systematically because there are several syntactic variations, write modes, formats, partitioning options, bucketing, JDBC, tables, and Delta-specific writes.

# 1. First understand `df.write`

When you have:

```python
df = spark.read.parquet("/data/employees")
```

`df` is a DataFrame.

To write it:

```python
df.write
```

`df.write` returns a **DataFrameWriter** object.

The general pattern is:

```python
df.write \
    .format("<format>") \
    .mode("<mode>") \
    .option("<key>", "<value>") \
    .save("<path>")
```

For example:

```python
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("/data/employees")
```

---

# 2. The complete `DataFrameWriter` structure

Think of Spark write operations like this:

```text
                         df.write
                            │
          ┌─────────────────┼──────────────────┐
          │                 │                  │
       FORMAT             MODE              OPTIONS
          │                 │                  │
    ┌─────┼─────┐      append             header
    │     │     │      overwrite           delimiter
 parquet  csv  delta    ignore              compression
 json     jdbc orc      error              ...
          │
          ↓
       OUTPUT
          │
    ┌─────┼──────────┐
    │     │          │
   save  table     jdbc
    │
    ↓
   PATH
```

There are also:

```python
partitionBy()
bucketBy()
sortBy()
```

for controlling physical organization.

---

# 3. Generic `format().save()`

This is the most important write syntax.

```python
df.write \
    .format("parquet") \
    .save("/data/employees")
```

Equivalent for Delta:

```python
df.write \
    .format("delta") \
    .save("/data/employees")
```

CSV:

```python
df.write \
    .format("csv") \
    .save("/data/employees")
```

JSON:

```python
df.write \
    .format("json") \
    .save("/data/employees")
```

The general syntax:

```python
df.write.format("FORMAT").save("PATH")
```

---

# 4. Format-specific shortcut methods

Spark provides convenience methods.

## Parquet

```python
df.write.parquet("/data/employees")
```

Equivalent to:

```python
df.write \
    .format("parquet") \
    .save("/data/employees")
```

---

## CSV

```python
df.write.csv("/data/employees")
```

Equivalent to:

```python
df.write \
    .format("csv") \
    .save("/data/employees")
```

---

## JSON

```python
df.write.json("/data/employees")
```

---

## ORC

```python
df.write.orc("/data/employees")
```

---

## Text

```python
df.write.text("/data/employees")
```

There is an important restriction here:

> `text()` expects a DataFrame with a single string column.

For example:

```python
df.select("name").write.text("/data/names")
```

---

# 5. Delta write

For Databricks, Delta is extremely important.

```python
df.write \
    .format("delta") \
    .save("/data/employees")
```

You can also use:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/data/employees")
```

Delta table:

```text
employees/
│
├── part-00000-....parquet
├── part-00001-....parquet
│
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    └── ...
```

The Parquet files contain the data.

`_delta_log` tracks the table's transactional state.

---

# 6. `saveAsTable()`

Instead of writing to a path, you can write to a **catalog table**.

```python
df.write \
    .format("delta") \
    .saveAsTable("employees")
```

Now you can query:

```sql
SELECT *
FROM employees;
```

With a database/schema:

```python
df.write \
    .format("delta") \
    .saveAsTable("silver.employees")
```

Then:

```sql
SELECT *
FROM silver.employees;
```

---

# 7. `insertInto()`

Another table-writing method is:

```python
df.write.insertInto("employees")
```

This inserts the DataFrame into an existing table.

For example:

```python
df.write \
    .mode("append") \
    .insertInto("employees")
```

### Important difference

`insertInto()` is intended for an **existing table** and has historically been associated with **position-based column resolution**, so you should be careful about column order.

For interview purposes:

> `saveAsTable()` is commonly used to create/write a catalog table, whereas `insertInto()` inserts into an existing table.

---

# 8. JDBC write

You can write a DataFrame to databases using JDBC.

```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "employees") \
    .option("user", username) \
    .option("password", password) \
    .save()
```

Or:

```python
df.write.jdbc(
    url=jdbc_url,
    table="employees",
    mode="append",
    properties=properties
)
```

Example:

```python
properties = {
    "user": "admin",
    "password": "password",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

df.write.jdbc(
    url="jdbc:oracle:thin:@//host:1521/service",
    table="EMPLOYEES",
    mode="append",
    properties=properties
)
```

---

# 9. Write modes

This is one of the most important parts of `df.write`.

Spark supports:

```text
append
overwrite
ignore
error / errorifexists
```

---

## `append`

```python
df.write \
    .mode("append") \
    .parquet("/data/employees")
```

Meaning:

> Add the new data to the existing data.

Existing:

```text
101 Mohan
102 Ravi
```

New:

```text
103 John
104 Kiran
```

After append:

```text
101 Mohan
102 Ravi
103 John
104 Kiran
```

---

# 10. `overwrite`

```python
df.write \
    .mode("overwrite") \
    .parquet("/data/employees")
```

Meaning:

> Replace the existing data at the target according to the semantics of the source/table being written.

Existing:

```text
101 Mohan
102 Ravi
```

New:

```text
103 John
104 Kiran
```

After overwrite:

```text
103 John
104 Kiran
```

### Important interview point

Don't say:

> "Overwrite deletes the directory first and then writes."

That's an oversimplification.

Spark's overwrite behavior depends on the data source and options. For file-based writes, Spark coordinates the write and replacement behavior through its output commit mechanism.

---

# 11. `ignore`

```python
df.write \
    .mode("ignore") \
    .parquet("/data/employees")
```

Meaning:

> If the target already exists, don't write anything.

If target doesn't exist:

```text
WRITE
```

If target exists:

```text
DO NOTHING
```

---

# 12. `errorIfExists`

```python
df.write \
    .mode("errorIfExists") \
    .parquet("/data/employees")
```

If the target already exists:

```text
ERROR
```

`error` is an alias commonly used for this mode:

```python
df.write.mode("error")
```

---

# 13. Write modes summary

| Mode            | Target exists                         | Target doesn't exist |
| --------------- | ------------------------------------- | -------------------- |
| `append`        | Add data                              | Create/write         |
| `overwrite`     | Replace according to source semantics | Create/write         |
| `ignore`        | Do nothing                            | Create/write         |
| `error`         | Error                                 | Create/write         |
| `errorifexists` | Error                                 | Create/write         |

---

# 14. CSV write with options

```python
df.write \
    .format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .save("/data/employees")
```

Or:

```python
df.write \
    .option("header", True) \
    .csv("/data/employees")
```

Common CSV options:

```python
.option("header", "true")
.option("delimiter", ",")
.option("quote", '"')
.option("escape", '"')
.option("nullValue", "NULL")
.option("emptyValue", "")
```

---

# 15. JSON write

```python
df.write \
    .format("json") \
    .mode("overwrite") \
    .save("/data/employees")
```

Or:

```python
df.write.json("/data/employees")
```

---

# 16. Parquet write with compression

```python
df.write \
    .format("parquet") \
    .option("compression", "snappy") \
    .save("/data/employees")
```

Common compression codecs can include:

```text
snappy
gzip
lz4
zstd
none
```

The available codecs depend on the format and Spark/Hadoop environment.

---

# 17. Partitioned write — extremely important

Suppose:

```text
emp_id
name
department
salary
```

You want data physically partitioned by department.

```python
df.write \
    .format("parquet") \
    .partitionBy("department") \
    .save("/data/employees")
```

Spark creates a structure like:

```text
employees/
│
├── department=IT/
│   ├── part-00000.parquet
│   └── part-00001.parquet
│
├── department=HR/
│   └── part-00000.parquet
│
└── department=Finance/
    └── part-00000.parquet
```

---

# 18. Why use `partitionBy()`?

Suppose you query:

```python
df.filter("department = 'IT'")
```

Spark can potentially read only:

```text
department=IT/
```

instead of scanning:

```text
IT
HR
Finance
Marketing
...
```

This is called **partition pruning**.

---

# 19. Multiple partition columns

You can partition by multiple columns:

```python
df.write \
    .partitionBy("year", "month") \
    .parquet("/data/sales")
```

Directory:

```text
sales/
│
├── year=2025/
│   ├── month=01/
│   ├── month=02/
│   └── month=03/
│
└── year=2026/
    ├── month=01/
    └── month=02/
```

---

# 20. `partitionBy()` vs Spark partitions

This is a **very important interview distinction**.

### Spark execution partition

```python
df.repartition(10)
```

controls how data is distributed across Spark tasks.

### Output partitioning

```python
df.write.partitionBy("department")
```

controls the **directory structure of the output data**.

They are not the same thing.

```text
repartition()
     ↓
Spark execution/data distribution

partitionBy()
     ↓
Output directory organization
```

---

# 21. `repartition()` before writing

You can control Spark partitions before the write:

```python
df.repartition(10) \
  .write \
  .parquet("/data/employees")
```

This can affect the number and distribution of output files.

But don't confuse:

```python
df.repartition(10)
```

with:

```python
df.write.partitionBy("department")
```

---

# 22. `coalesce()` before writing

You can also reduce the number of partitions:

```python
df.coalesce(5) \
  .write \
  .parquet("/data/employees")
```

Common use case:

```text
Huge number of small output files
             ↓
       coalesce()
             ↓
    fewer output files
```

Be careful: `coalesce()` generally avoids a full shuffle when reducing partitions, whereas `repartition()` performs a shuffle.

---

# 23. Bucketing

Spark also supports:

```python
df.write \
    .bucketBy(10, "emp_id") \
    .saveAsTable("employees")
```

This creates **10 buckets** based on the bucket expression/column.

You can also combine:

```python
df.write \
    .partitionBy("department") \
    .bucketBy(10, "emp_id") \
    .sortBy("emp_id") \
    .saveAsTable("employees")
```

Conceptually:

```text
employees
│
├── department=IT
│    ├── bucket 0
│    ├── bucket 1
│    └── ...
│
└── department=HR
     ├── bucket 0
     ├── bucket 1
     └── ...
```

### Important

`bucketBy()` is generally used with `saveAsTable()` rather than arbitrary file-path writes.

---

# 24. `sortBy()`

You can sort data within buckets:

```python
df.write \
    .bucketBy(10, "emp_id") \
    .sortBy("salary") \
    .saveAsTable("employees")
```

Think:

```text
partitionBy()
    ↓
Directory-level organization

bucketBy()
    ↓
Hash-based buckets

sortBy()
    ↓
Sort within buckets
```

---

# 25. Write a temporary view?

This is slightly different.

If your goal is simply to make a DataFrame available to SQL:

```python
df.createOrReplaceTempView("employees")
```

Then:

```python
spark.sql("""
    SELECT *
    FROM employees
""")
```

This **does not write the DataFrame to persistent storage**.

This is an important distinction:

```text
df.write
    ↓
Persistent output

createOrReplaceTempView()
    ↓
SQL view for the Spark session
```

---

# 26. Write using SQL

Instead of:

```python
df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("employees")
```

you can use SQL:

```sql
INSERT INTO employees
SELECT *
FROM source_employees;
```

Or:

```sql
CREATE TABLE employees
USING DELTA
AS
SELECT *
FROM source_employees;
```

This is another important way of writing data in Spark/Databricks, although technically you're using **Spark SQL**, not the Python `DataFrameWriter` API.

---

# 27. CTAS — Create Table As Select

For example:

```sql
CREATE TABLE employees
USING DELTA
AS
SELECT *
FROM source_employees;
```

This:

1. Creates the table.
2. Executes the query.
3. Writes the resulting data.
4. Registers the table.

---

# 28. `INSERT INTO`

Existing table:

```sql
INSERT INTO employees
SELECT *
FROM source_employees;
```

This is equivalent conceptually to:

```python
df.write \
    .mode("append") \
    .saveAsTable("employees")
```

---

# 29. `INSERT OVERWRITE`

You can also use:

```sql
INSERT OVERWRITE employees
SELECT *
FROM source_employees;
```

Conceptually this corresponds to replacing the target data according to the SQL/table semantics.

---

# 30. Delta `MERGE`

For Delta Lake, there is another extremely important write pattern.

Suppose:

```text
target:
101 Mohan 60000
102 Ravi  50000

source:
101 Mohan 65000
103 John  70000
```

Use:

```sql
MERGE INTO target t
USING source s
ON t.emp_id = s.emp_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *;
```

Result:

```text
101 Mohan 65000   ← UPDATE
102 Ravi  50000   ← unchanged
103 John  70000   ← INSERT
```

This is extremely common in **incremental data pipelines**.

---

# 31. `replaceWhere`

For file-based writes and Delta in particular, `replaceWhere` can be useful for selectively replacing data matching a predicate.

For example:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "year = 2026") \
    .save("/data/sales")
```

Conceptually:

```text
Existing:

2025 data → keep
2026 data → replace
```

rather than blindly replacing the entire table.

This is useful for **partition-specific overwrites**.

---

# 32. Dynamic partition overwrite

For partitioned data, another pattern is:

```python
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)
```

Then:

```python
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("/data/sales")
```

The intent is to overwrite only partitions represented by the incoming data rather than every partition.

For example, existing:

```text
year=2025/month=01
year=2025/month=02
year=2026/month=01
year=2026/month=02
```

Incoming data:

```text
year=2026/month=02
```

With dynamic partition overwrite, the relevant partition can be replaced while other partitions remain.

---

# 33. Delta `overwriteSchema`

Suppose the existing Delta schema is:

```text
id       INT
name     STRING
salary   DOUBLE
```

Your incoming DataFrame has:

```text
id       INT
name     STRING
salary   STRING
```

With an appropriate overwrite:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("/data/employees")
```

This tells Delta that the overwrite is intended to replace the existing table schema as well, subject to the supported schema-change rules.

---

# 34. Delta `mergeSchema`

For adding compatible columns:

```python
df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("/data/employees")
```

Example:

Existing:

```text
id
name
salary
```

Incoming:

```text
id
name
salary
department
```

With schema evolution enabled appropriately:

```text
id
name
salary
department
```

---

# 35. `option()` vs `options()`

You can specify one option:

```python
df.write \
    .option("header", "true") \
    .csv("/data/employees")
```

Or multiple options using `options()`:

```python
df.write \
    .options(
        header="true",
        delimiter=",",
        compression="gzip"
    ) \
    .csv("/data/employees")
```

---

# 36. `partitionBy()` with Delta

Very common in Databricks:

```python
df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year", "month") \
    .save("/data/sales")
```

Storage could look like:

```text
sales/
│
├── _delta_log/
│
├── year=2025/
│   ├── month=01/
│   └── month=02/
│
└── year=2026/
    ├── month=01/
    └── month=02/
```

---

# 37. Writing to S3

The same APIs work with cloud storage paths.

```python
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("s3://my-bucket/employees")
```

Delta:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://my-bucket/delta/employees")
```

Azure:

```python
df.write \
    .format("delta") \
    .save("abfss://container@storage.dfs.core.windows.net/employees")
```

The exact authentication mechanism depends on the platform.

---

# 38. Writing with a custom format

Because Spark uses a data-source architecture, you aren't limited to:

```text
csv
json
parquet
orc
delta
```

You can use a supported/custom data source:

```python
df.write \
    .format("some_format") \
    .option("someOption", "value") \
    .save("/data/output")
```

This is why:

```python
format()
```

is so powerful.

---

# 39. Complete write syntax cheat sheet

### Generic

```python
df.write \
    .format("format") \
    .mode("mode") \
    .option("key", "value") \
    .save("path")
```

### Parquet

```python
df.write.parquet(path)
```

### CSV

```python
df.write.csv(path)
```

### JSON

```python
df.write.json(path)
```

### ORC

```python
df.write.orc(path)
```

### Text

```python
df.write.text(path)
```

### Delta

```python
df.write.format("delta").save(path)
```

### Table

```python
df.write.saveAsTable("database.table")
```

### Existing table

```python
df.write.insertInto("database.table")
```

### JDBC

```python
df.write.jdbc(
    url,
    table,
    mode,
    properties
)
```

### Partition

```python
df.write.partitionBy("column").parquet(path)
```

### Bucket

```python
df.write \
    .bucketBy(10, "id") \
    .saveAsTable("employees")
```

### Bucket + sort

```python
df.write \
    .bucketBy(10, "id") \
    .sortBy("id") \
    .saveAsTable("employees")
```

---

# 40. The most important distinction: `save()` vs `saveAsTable()` vs `insertInto()`

This is a very common interview question.

| Method                           | Purpose                        |
| -------------------------------- | ------------------------------ |
| `save(path)`                     | Write data to a storage path   |
| `saveAsTable(table)`             | Create/write a catalog table   |
| `insertInto(table)`              | Insert into an existing table  |
| `jdbc()`                         | Write to a JDBC database       |
| `parquet()` / `csv()` / `json()` | Convenience file-format writes |

### Example

```python
df.write.format("delta").save("/data/employees")
```

Path-based:

```text
/data/employees
```

Whereas:

```python
df.write.format("delta").saveAsTable("employees")
```

Table-based:

```text
catalog
   ↓
employees
```

---

# 41. What happens behind the scenes when you write?

This is particularly important for your **PySpark + Delta Lake interviews**.

Suppose:

```python
df.write \
    .format("delta") \
    .mode("append") \
    .save("/data/employees")
```

Conceptually:

```text
DataFrame
   ↓
Logical plan
   ↓
Spark execution
   ↓
Tasks
   ↓
Each task writes output
   ↓
Parquet files
   ↓
Delta transaction commit
   ↓
_delta_log
```

For example:

```text
/data/employees/

part-00000-A.parquet
part-00001-B.parquet
part-00002-C.parquet

_delta_log/
00000000000000000000.json
```

The Parquet files contain the actual rows.

The Delta transaction log records the committed table state.

---

# 42. Why do multiple Parquet files get created?

Suppose:

```python
df.rdd.getNumPartitions()
```

returns:

```text
10
```

Spark may have multiple tasks writing output concurrently.

Conceptually:

```text
Partition 0 → task 0 → part-00000
Partition 1 → task 1 → part-00001
Partition 2 → task 2 → part-00002
...
```

Therefore:

> **One DataFrame does not necessarily result in one output file.**

This is why you may see:

```text
part-00000
part-00001
part-00002
...
```

---

# 43. How to control output file count

### `coalesce()`

```python
df.coalesce(1) \
    .write \
    .parquet("/data/employees")
```

Potentially produces one output partition/file.

But **don't routinely use `coalesce(1)` on large datasets** because it can create a bottleneck.

### `repartition()`

```python
df.repartition(10) \
    .write \
    .parquet("/data/employees")
```

Can distribute the data into 10 partitions before writing.

---

# 44. A realistic production example

Suppose you're loading Salesforce data into a Delta Silver table.

```python
source_df = spark.read \
    .format("parquet") \
    .load("s3://raw/salesforce/customer")
```

Transform:

```python
from pyspark.sql.functions import *

silver_df = source_df \
    .filter(col("is_deleted") == False) \
    .dropDuplicates(["customer_id"])
```

Write:

```python
silver_df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("country") \
    .save("s3://silver/customer")
```

Backend conceptually:

```text
Salesforce
    ↓
Raw S3
    ↓
PySpark
    ↓
Transformations
    ↓
DataFrame
    ↓
partitionBy(country)
    ↓
Spark tasks
    ↓
Parquet files
    ↓
Delta transaction
    ↓
_delta_log
```

---

# 45. Most important write commands for your interviews

If you're preparing specifically for **PySpark Data Engineering interviews**, prioritize these:

### Level 1 — Must know

```python
df.write.format("parquet").save(path)

df.write.parquet(path)

df.write.csv(path)

df.write.json(path)

df.write.format("delta").save(path)
```

### Level 2 — Very important

```python
df.write.mode("append").save(path)

df.write.mode("overwrite").save(path)

df.write.mode("ignore").save(path)

df.write.mode("error").save(path)
```

### Level 3 — Production

```python
df.write \
    .partitionBy("year", "month") \
    .format("parquet") \
    .save(path)
```

```python
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(path)
```

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(path)
```

### Level 4 — Tables

```python
df.write.saveAsTable("db.table")

df.write.insertInto("db.table")
```

### Level 5 — Database

```python
df.write.jdbc(
    url,
    table,
    mode,
    properties
)
```

### Level 6 — Advanced

```python
df.write \
    .partitionBy("country") \
    .bucketBy(10, "customer_id") \
    .sortBy("customer_id") \
    .saveAsTable("customers")
```

---

# ⭐ One-page mental model

```text
                         df.write
                            │
        ┌───────────────────┼────────────────────┐
        │                   │                    │
      FORMAT              MODE                OPTIONS
        │                   │                    │
   parquet               append              header
   csv                   overwrite            compression
   json                  ignore               mergeSchema
   orc                   error                overwriteSchema
   delta
   jdbc
        │
        ↓
   ┌────┴─────────┐
   │              │
 save()       saveAsTable()
   │              │
 PATH          CATALOG TABLE
   │              │
   └──────┬───────┘
          │
     partitionBy()
          │
     bucketBy()
          │
       sortBy()
```

### The interview sentence to remember

> **`DataFrameWriter` provides multiple ways to persist a DataFrame. The generic pattern is `df.write.format(...).mode(...).option(...).save(path)`. Spark also provides convenience methods such as `parquet()`, `csv()`, `json()`, and `orc()`, table-based methods such as `saveAsTable()` and `insertInto()`, and `jdbc()` for relational databases. `partitionBy()` controls the physical directory organization, while `repartition()` and `coalesce()` control Spark's execution partitions and therefore can influence the number of output files. For Delta Lake, writes additionally update the Delta transaction log, enabling transactional table state, schema management, and other Delta features.**
