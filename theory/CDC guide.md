# Complete guide: Implementing CDC using PySpark and SQL

CDC (Change Data Capture) is one of the most important concepts in data engineering, especially when building incremental data pipelines using PySpark, Databricks, Spark SQL, and Delta Lake.

I'll explain how to implement CDC from the source to the target, including INSERT, UPDATE, DELETE, handling duplicate records, and maintaining incremental loads.

We'll cover two major implementation approaches:

1. CDC using source timestamps – Identify changes using `created_date` and `updated_date`.
2. CDC using change logs – Process INSERT, UPDATE, and DELETE events received from the source.

We'll also implement these using PySpark and SQL.

## 1. First understand the CDC architecture

Imagine you have a Salesforce employee table with 10 million records.

Every day:

- 1,000 new employees are inserted.
- 500 existing employees are updated.
- 100 employees are deleted.

Without CDC, you might read all 10 million records every day.

With CDC, you process only the changes, provided your source exposes a reliable way to identify them.

Source system

Salesforce / MySQL / PostgreSQL

INSERT · UPDATE · DELETE

CDC extraction

Capture only changed records

Bronze layer

Store raw change events

Silver layer

Deduplicate · Validate · Order events

Apply CDC

Target Delta table

Insert · Update · Delete

Important distinction: CDC is not just a SQL `MERGE` statement. CDC includes identifying changes, extracting them, transporting them, ordering them, and applying them to the target.

## 2. Understand the source data before implementation

Let's create a source employee table.

Initial source data:

| emp_id | emp_name | department | salary |
| ------ | -------- | ---------- | ------ |
| 101    | Ravi     | IT         | 30000  |
| 102    | Kumar    | HR         | 40000  |
| 103    | Raj      | Finance    | 50000  |

Assume the source undergoes these changes:

| Operation | emp_id | emp_name | department | salary |
| --------- | ------ | -------- | ---------- | ------ |
| INSERT    | 104    | Arun     | IT         | 35000  |
| UPDATE    | 101    | Ravi     | HR         | 35000  |
| DELETE    | 103    | Raj      | Finance    | 50000  |

After CDC processing, the target should contain:

| emp_id | emp_name | department | salary |
| ------ | -------- | ---------- | ------ |
| 101    | Ravi     | HR         | 35000  |
| 102    | Kumar    | HR         | 40000  |
| 104    | Arun     | IT         | 35000  |

Notice:

- Employee 104 was inserted.
- Employee 101 was updated.
- Employee 103 was deleted.

Now let's implement this.

## 3. Method 1: Implement CDC using timestamps

This approach is commonly called timestamp-based incremental extraction.

Suppose your source table contains:

```
CREATE TABLE employee_source (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary DOUBLE,
    created_date TIMESTAMP,
    updated_date TIMESTAMP
);
```

The source maintains:

- `created_date`: When the record was initially inserted.
- `updated_date`: When the record was last modified.

For example:

| emp_id | emp_name | created_date | updated_date |
| ------ | -------- | ------------ | ------------ |
| 101    | Ravi     | Jan 1        | Jan 1        |
| 102    | Kumar    | Jan 2        | Jan 2        |

When Ravi's salary changes, the source updates `updated_date`.

### Step 1: Maintain a watermark

A watermark is the last successfully processed timestamp.

For example:

```
last_processed_timestamp = "2026-10-30 00:00:00"
```

### Step 2: Extract incremental records using PySpark

```
from pyspark.sql.functions import colincremental_df = source_df.filter(    col("updated_date") > last_processed_timestamp)
```

This retrieves only records modified after the previous successful extraction.

Equivalent SQL:

```
SELECT *
FROM employee_source
WHERE updated_date > TIMESTAMP '2026-10-30 00:00:00';
```

### Step 3: Write the incremental records to Bronze

```
incremental_df.write \    .format("delta") \    .mode("append") \    .saveAsTable("bronze.employee_cdc")
```

### Step 4: Apply changes to Silver using MERGE

```
MERGE INTO silver.employee_target AS target
USING bronze.employee_cdc AS source

ON target.emp_id = source.emp_id

WHEN MATCHED THEN
UPDATE SET
    target.emp_name = source.emp_name,
    target.department = source.department,
    target.salary = source.salary

WHEN NOT MATCHED THEN
INSERT (
    emp_id,
    emp_name,
    department,
    salary
)
VALUES (
    source.emp_id,
    source.emp_name,
    source.department,
    source.salary
);
```

This handles inserts and updates.

Limitation: Timestamp-based extraction cannot reliably detect hard deletes unless the source provides a deletion timestamp, a soft-delete flag, or another mechanism to identify deleted records.

Also, using only `updated_date` requires care: timestamps must be reliably maintained, and records with equal timestamps must not be skipped.

### Important problem with timestamp-based CDC

Suppose the same employee is updated twice:

| emp_id | salary | updated_date |
| ------ | ------ | ------------ |
| 101    | 35000  | 10:00        |
| 101    | 40000  | 11:00        |

If both records are extracted, you should not blindly merge both without considering event ordering.

You should retain the latest event for each employee before applying it.

PySpark:

```
from pyspark.sql.window import Windowfrom pyspark.sql.functions import row_number, col, descwindow_spec = Window.partitionBy("emp_id").orderBy(    desc("updated_date"))latest_df = (    incremental_df    .withColumn("rn", row_number().over(window_spec))    .filter(col("rn") == 1)    .drop("rn"))
```

This produces only the latest record per employee.

Limitation: This approach is suitable for maintaining the current state. If you need every intermediate change for auditing, retain all events in Bronze instead of discarding older events.

## 4. Method 2: CDC using operation types (INSERT, UPDATE, DELETE)

This is a more complete CDC implementation.

In this approach, the source provides a CDC feed containing:

- Primary key
- Operation type
- Changed column values
- Event timestamp or sequence number

For example, a CDC feed from Debezium or a database's native CDC mechanism.

Let's create sample data.

### Step 1: Create a CDC DataFrame

```
data = [    (101, "Ravi", "HR", 35000, "UPDATE", 2),    (102, "Kumar", "HR", 40000, "DELETE", 3),    (104, "Arun", "IT", 35000, "INSERT", 1),    (101, "Ravi", "IT", 30000, "UPDATE", 1)]columns = [    "emp_id",    "emp_name",    "department",    "salary",    "operation",    "sequence_number"]cdc_df = spark.createDataFrame(data, columns)
```

Here, `sequence_number` represents the order of changes for a particular employee.

Our data contains:

| emp_id | department | operation | sequence_number |
| ------ | ---------- | --------- | --------------- |
| 101    | HR         | UPDATE    | 2               |
| 102    | HR         | DELETE    | 3               |
| 104    | IT         | INSERT    | 1               |
| 101    | IT         | UPDATE    | 1               |

Notice that employee 101 has two updates.

The first update changed the department to IT, and the second update changed it to HR.

### Step 2: Deduplicate and identify the latest event

```
from pyspark.sql.window import Windowfrom pyspark.sql.functions import row_number, col, descwindow_spec = Window.partitionBy("emp_id").orderBy(    desc("sequence_number"))latest_cdc_df = (    cdc_df    .withColumn(        "rn",        row_number().over(window_spec)    )    .filter(col("rn") == 1)    .drop("rn"))
```

Result:

| emp_id | department | operation | sequence_number |
| ------ | ---------- | --------- | --------------- |
| 101    | HR         | UPDATE    | 2               |
| 102    | HR         | DELETE    | 3               |
| 104    | IT         | INSERT    | 1               |

Now we have one latest event per employee.

Important: This deduplication is appropriate when your objective is to apply the latest state. If your requirement is to preserve every change, process all events in sequence instead.

### Step 3: Create the target Delta table

```
CREATE TABLE IF NOT EXISTS silver.employee_target (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary DOUBLE,
    last_sequence_number BIGINT
)
USING DELTA;
```

### Step 4: Apply CDC using SQL MERGE

This is the most important step.

```
MERGE INTO silver.employee_target AS target

USING latest_cdc AS source

ON target.emp_id = source.emp_id

WHEN MATCHED
AND source.sequence_number > target.last_sequence_number
AND source.operation = 'UPDATE'

THEN UPDATE SET
    target.emp_name = source.emp_name,
    target.department = source.department,
    target.salary = source.salary,
    target.last_sequence_number = source.sequence_number

WHEN MATCHED
AND source.sequence_number > target.last_sequence_number
AND source.operation = 'DELETE'

THEN DELETE

WHEN NOT MATCHED
AND source.operation = 'INSERT'

THEN INSERT (
    emp_id,
    emp_name,
    department,
    salary,
    last_sequence_number
)
VALUES (
    source.emp_id,
    source.emp_name,
    source.department,
    source.salary,
    source.sequence_number
);
```

What happens?

| Operation | MERGE action              |
| --------- | ------------------------- |
| INSERT    | Insert a new record       |
| UPDATE    | Update an existing record |
| DELETE    | Delete an existing record |

The sequence condition prevents an older update from overwriting a newer target record.

One additional production consideration: if a key is deleted and later reinserted, retain deletion sequence information or a tombstone. Otherwise, an older delayed insert could accidentally recreate the deleted record.

### Step 5: Implement the same MERGE using PySpark

You can use the Delta Lake `DeltaTable` API.

```
from delta.tables import DeltaTabletarget = DeltaTable.forName(    spark,    "silver.employee_target")(    target.alias("t")    .merge(        latest_cdc_df.alias("s"),        "t.emp_id = s.emp_id"    )    .whenMatchedUpdate(        condition="""            s.sequence_number > t.last_sequence_number            AND s.operation = 'UPDATE'        """,        set={            "emp_name": "s.emp_name",            "department": "s.department",            "salary": "s.salary",            "last_sequence_number": "s.sequence_number"        }    )    .whenMatchedDelete(        condition="""            s.sequence_number > t.last_sequence_number            AND s.operation = 'DELETE'        """    )    .whenNotMatchedInsert(        condition="s.operation = 'INSERT'",        values={            "emp_id": "s.emp_id",            "emp_name": "s.emp_name",
```

This is the PySpark implementation of CDC using Delta Lake.

Note that the SQL and PySpark examples above use a simplified event model in which each event contains a complete after-image of the record. Real CDC feeds can have partial updates and different before/after-image conventions.

## 5. Method 3: CDC using Delta Change Data Feed (CDF)

This is particularly important if you work with Databricks.

Delta Lake provides a feature called Change Data Feed (CDF).

It captures row-level changes made to a Delta table.

For example:

- INSERT
- UPDATE
- DELETE

Instead of manually identifying these changes, Delta Lake provides change records that you can read.

### Step 1: Enable CDF

For a new Delta table:

```
CREATE TABLE employee_source (
    emp_id INT,
    emp_name STRING,
    department STRING,
    salary DOUBLE
)
USING DELTA
TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

For an existing table:

```
ALTER TABLE employee_source
SET TBLPROPERTIES (
    delta.enableChangeDataFeed = true
);
```

### Step 2: Read the changes using PySpark

```
cdf_df = (    spark.read    .format("delta")    .option("readChangeFeed", "true")    .option("startingVersion", 10)    .option("endingVersion", 20)    .table("employee_source"))
```

CDF adds metadata columns:

| Column              | Meaning                   |
| ------------------- | ------------------------- |
| `_change_type`      | Type of change            |
| `_commit_version`   | Delta transaction version |
| `_commit_timestamp` | Commit timestamp          |

Possible `_change_type` values:

| Change type      | Meaning              |
| ---------------- | -------------------- |
| insert           | New record           |
| update_preimage  | Record before update |
| update_postimage | Record after update  |
| delete           | Deleted record       |

### Step 3: Filter the required changes

```
from pyspark.sql.functions import colinsert_df = cdf_df.filter(    col("_change_type") == "insert")update_df = cdf_df.filter(    col("_change_type") == "update_postimage")delete_df = cdf_df.filter(    col("_change_type") == "delete")
```

You generally use `update_postimage` when applying the latest updated state. `update_preimage` represents the old state and is useful for auditing or change comparisons.

### Step 4: Read CDF using SQL

```
SELECT *
FROM table_changes(
    'employee_source',
    10,
    20
);
```

You can also filter:

```
SELECT *
FROM table_changes(
    'employee_source',
    10,
    20
)
WHERE _change_type IN (
    'insert',
    'update_postimage',
    'delete'
);
```

CDF is especially useful when one Delta table acts as a source for another Delta table.

Important: CDF must be enabled before the changes you want to capture. It does not automatically reconstruct changes that happened before it was enabled. Retention and table history also determine how long you can read the change feed.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.delta.io\&sz=32)

Delta Lake+1



## 6. Method 4: Implement CDC using Databricks AUTO CDC

If you are using Databricks Lakeflow Declarative Pipelines, there is a more convenient option: `AUTO CDC`.

It automates several difficult CDC operations:

- Identifying the latest event for each key.
- Processing events in the correct sequence.
- Handling out-of-order events.
- Applying INSERT, UPDATE, and DELETE.
- Maintaining SCD Type 1 or Type 2.

Databricks recommends `AUTO CDC` instead of the older `APPLY CHANGES` APIs.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



### A. AUTO CDC using Python

Assume your Bronze table contains:

| emp_id | emp_name | department | operation | sequence_number |
| ------ | -------- | ---------- | --------- | --------------- |
| 101    | Ravi     | HR         | UPDATE    | 2               |
| 102    | Kumar    | HR         | DELETE    | 3               |
| 104    | Arun     | IT         | INSERT    | 1               |

In a Lakeflow pipeline:

```
from pyspark import pipelines as dpfrom pyspark.sql.functions import col, expr@dp.viewdef employee_changes():    return spark.readStream.table(        "bronze.employee_cdc"    )dp.create_streaming_table(    "employee_current")dp.create_auto_cdc_flow(    target="employee_current",    source="employee_changes",    keys=["emp_id"],    sequence_by=col("sequence_number"),    apply_as_deletes=expr(        "operation = 'DELETE'"    ),    except_column_list=[        "operation",        "sequence_number"    ],    stored_as_scd_type=1)
```

What does each parameter do?

| Parameter            | Purpose                            |
| -------------------- | ---------------------------------- |
| `target`             | Destination streaming table        |
| `source`             | Incoming CDC records               |
| `keys`               | Primary key identifying records    |
| `sequence_by`        | Determines the order of changes    |
| `apply_as_deletes`   | Identifies delete events           |
| `except_column_list` | Excludes CDC metadata from target  |
| `stored_as_scd_type` | Defines how records are maintained |

For SCD Type 2, change:

```
stored_as_scd_type=2
```

Now the target maintains historical versions instead of overwriting the old versions.

### B. AUTO CDC using SQL

You can achieve the same functionality using SQL in a Lakeflow pipeline.

```
CREATE OR REFRESH STREAMING TABLE employee_current;

CREATE FLOW employee_cdc_flow AS AUTO CDC INTO employee_current

FROM employee_changes

KEYS (emp_id)

APPLY AS DELETE WHEN operation = 'DELETE'

SEQUENCE BY sequence_number

COLUMNS * EXCEPT (
    operation,
    sequence_number
)

STORED AS SCD TYPE 1;
```

For historical tracking:

```
STORED AS SCD TYPE 2;
```

Important: AUTO CDC is a Lakeflow Pipelines feature, not an ordinary SQL statement that you can execute in any Spark SQL session. It requires a supported Lakeflow pipeline configuration and edition.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## 7. How to handle important CDC scenarios

In real-time projects, simply writing a MERGE statement is not enough. You must handle the following scenarios.

### Scenario 1: Duplicate CDC records

Suppose the source sends:

| emp_id | salary | sequence |
| ------ | ------ | -------- |
| 101    | 35000  | 2        |
| 101    | 35000  | 2        |
| 101    | 40000  | 3        |

You should identify duplicate events and process the latest valid event.

```
dedup_df = cdc_df.dropDuplicates([    "emp_id",    "sequence_number",    "operation"])
```

Then use the window function to get the latest event per key.

Note: This assumes the sequence number uniquely identifies an event for a given employee. If it doesn't, use a source event ID or transaction ID for deduplication.

### Scenario 2: Out-of-order records

Suppose records arrive in this order:

| emp_id | salary | sequence |
| ------ | ------ | -------- |
| 101    | 40000  | 3        |
| 101    | 35000  | 2        |

The newer event arrived first.

If you blindly apply both events, the older event could overwrite the newer one.

Solution:

```
WHEN MATCHED
AND source.sequence_number > target.last_sequence_number
THEN UPDATE SET ...
```

This ensures that older events cannot overwrite newer events for existing target records.

### Scenario 3: Delete operation

Suppose an employee is deleted from the source.

There are two approaches:

Hard delete:

```
WHEN MATCHED
AND source.operation = 'DELETE'
THEN DELETE
```

The record is physically removed from the target's current-state table.

Soft delete:

```
WHEN MATCHED
AND source.operation = 'DELETE'
THEN UPDATE SET
    target.is_deleted = true
```

The record remains available for auditing.

For a soft-delete implementation, retain the delete sequence number and filter deleted records from normal business queries.

### Scenario 4: Multiple updates in one batch

Suppose one employee has five updates in the same batch.

You can use:

```
from pyspark.sql.window import Windowfrom pyspark.sql.functions import row_number, col, descw = Window.partitionBy("emp_id").orderBy(    desc("sequence_number"))latest_df = (    cdc_df    .withColumn("rn", row_number().over(w))    .filter(col("rn") == 1)    .drop("rn"))
```

This works when you only need the final state.

If you need the full history, don't discard the intermediate events.

## 8. How to maintain a watermark in production

This is an important part of incremental CDC pipelines.

Suppose yesterday's pipeline successfully processed data up to sequence number 1000.

Today, you should process records after sequence number 1000.

You need a control table.

```
CREATE TABLE IF NOT EXISTS metadata.cdc_watermark (
    table_name STRING,
    last_sequence_number BIGINT,
    last_processed_timestamp TIMESTAMP
)
USING DELTA;
```

Example:

| table_name | last_sequence_number |
| ---------- | -------------------- |
| employee   | 1000                 |

Read the watermark:

```
watermark_df = spark.table(    "metadata.cdc_watermark")last_sequence = (    watermark_df    .filter(col("table_name") == "employee")    .select("last_sequence_number")    .first()[0])
```

Extract incremental records:

```
incremental_df = source_df.filter(    col("sequence_number") > last_sequence)
```

After successfully processing and committing the target changes, update the watermark.

```
UPDATE metadata.cdc_watermark
SET last_sequence_number = 1500,
    last_processed_timestamp = CURRENT_TIMESTAMP()
WHERE table_name = 'employee';
```

Production warning: Updating the target and watermark are separate transactions in this example. If the target succeeds but the watermark update fails, the next run will replay events. Your CDC processing must therefore be idempotent. Also, use a source-provided committed offset or a composite watermark when sequence values are not globally ordered. Do not simply use `MAX(sequence_number)` if source events can arrive late.

A good production workflow is:

1. Read the last committed watermark.
2. Extract the next bounded batch.
3. Write the raw events to Bronze.
4. Deduplicate and validate the events.
5. Apply changes to Silver.
6. Confirm successful target processing.
7. Commit the watermark.
8. Record the batch status for recovery and monitoring.

## 9. Comparison of all CDC implementation approaches

| Approach            | PySpark | SQL | Delete support            |
| ------------------- | ------- | --- | ------------------------- |
| Timestamp-based CDC | Yes     | Yes | Requires additional logic |
| Operation-based CDC | Yes     | Yes | Yes                       |
| Delta CDF           | Yes     | Yes | Yes                       |
| Databricks AUTO CDC | Yes     | Yes | Yes                       |

One important distinction:

- Delta CDF reads changes made to Delta tables.
- Database CDC feeds capture changes from databases such as MySQL, SQL Server, and Oracle.
- AUTO CDC applies an incoming change feed to a target table.

These are related but not interchangeable concepts.&#x20;

[image](https://www.google.com/s2/favicons?domain=https://docs.databricks.com\&sz=32)

Databricks on AWS+1



## 10. What should you use in a real Databricks project?

Considering a Salesforce → AWS S3 → Databricks Medallion architecture, I would approach it like this:

| Layer      | Implementation                                                    |
| ---------- | ----------------------------------------------------------------- |
| Salesforce | Extract source changes using a reliable change-tracking mechanism |
| S3         | Store incremental files                                           |
| Bronze     | Append raw records with operation type and source sequence        |
| Silver     | Deduplicate, validate, and apply CDC                              |
| Gold       | Create business-ready current-state or historical tables          |

For your pipeline, there are two possibilities:

Case A: Salesforce provides only created and updated timestamps

Use timestamp-based incremental extraction. For deletes, you need a deletion flag, deletion feed, or periodic reconciliation with source snapshots.

Case B: Salesforce provides a reliable change event feed

Use the operation type, primary key, and event ordering information to implement full CDC.

Don't assume that an `updated_date` column alone captures deletes.

## 11. Interview questions you should prepare

| Interview question                       | Important concept                    |
| ---------------------------------------- | ------------------------------------ |
| What is CDC?                             | Incremental change capture           |
| How do you identify inserts and updates? | Operation type / timestamps          |
| How do you capture deletes?              | Delete events / tombstones           |
| How do you handle duplicate events?      | Event ID / deduplication             |
| How do you handle out-of-order events?   | Sequence number                      |
| What is a watermark?                     | Last committed extraction position   |
| What is Delta CDF?                       | Delta row-level change feed          |
| What is AUTO CDC?                        | Managed CDC processing               |
| Can we implement CDC without SCD?        | Yes                                  |
| Can we implement SCD without CDC?        | Yes, using snapshots and comparisons |

## 12. Official documentation for further learning

- [Databricks – Change Data Capture and Snapshots](https://docs.databricks.com/aws/en/data-engineering/what-is-cdc?utm_source=chatgpt.com)
- [Databricks – AUTO CDC APIs](https://docs.databricks.com/aws/en/ldp/cdc?utm_source=chatgpt.com)
- [Delta Lake – Change Data Feed](https://docs.delta.io/delta-change-data-feed/?utm_source=chatgpt.com)
- [Apache Spark – MERGE INTO](https://spark.apache.org/docs/latest/sql-ref-syntax-dml-merge-into.html?utm_source=chatgpt.com)

## My recommendation: Learn these three implementations thoroughly

1

PySpark + Delta MERGE

Understand how to implement INSERT, UPDATE, DELETE manually using DataFrames and DeltaTable.

2

SQL + MERGE INTO

Understand matched and not-matched conditions, sequence numbers, and duplicate handling.

3

Databricks AUTO CDC

Understand how to implement managed CDC with SCD Type 1 and Type 2 in Lakeflow Pipelines.

One important interview point: `MERGE` is the mechanism used to apply changes to a target table; CDC is the overall process of identifying, capturing, and applying those changes.

I can also build a complete end-to-end CDC project using sample CSV files, PySpark, SQL, Delta Lake, Bronze and Silver tables, including INSERT, UPDATE, DELETE, duplicate handling, and restart recovery.
