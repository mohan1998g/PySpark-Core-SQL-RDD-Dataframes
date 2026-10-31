CDC (Change Data Capture) and SCD (Slowly Changing Dimension) are two different data engineering concepts, but they are closely related.

- CDC: Identifies and captures changes happening in source data.
- SCD: Manages and stores changes in dimension tables, including historical changes when required.

Let's understand them from a PySpark, SQL, and Databricks perspective.

## 1. CDC – Change Data Capture

CDC is a technique used to identify changes in source data and capture only those changes instead of processing the entire table repeatedly.

There are generally three types of changes:

1. INSERT – A new record is inserted.
2. UPDATE – An existing record is modified.
3. DELETE – An existing record is deleted.

Example:

Suppose Salesforce has this employee table.

Initial source data:

| emp_id | name  | salary |
| ------ | ----- | ------ |
| 101    | Ravi  | 30000  |
| 102    | Kumar | 40000  |

After some time, the source changes:

- Ravi's salary changes from 30,000 to 35,000.
- A new employee Raj is inserted.
- Employee Kumar is deleted.

CDC captures these changes:

| emp_id | name  | salary | operation |
| ------ | ----- | ------ | --------- |
| 101    | Ravi  | 35000  | UPDATE    |
| 103    | Raj   | 45000  | INSERT    |
| 102    | Kumar | NULL   | DELETE    |

This is an example of CDC data.

How can we implement CDC?

Common approaches include:

- Database transaction logs (WAL, binlog, redo logs).
- Timestamp-based incremental extraction.
- Database CDC tools such as Debezium.
- Change tracking and change data feed mechanisms.

PySpark example:

Suppose CDC records are available in a DataFrame.

```
from pyspark.sql.functions import colinserts = cdc_df.filter(col("operation") == "INSERT")updates = cdc_df.filter(col("operation") == "UPDATE")deletes = cdc_df.filter(col("operation") == "DELETE")
```

Here, we identify the three types of changes.

## 2. SCD – Slowly Changing Dimension

SCD is a data warehousing technique used to manage changes in dimension tables.

For example, an employee changes their department from IT to HR.

How should we store this change?

There are different SCD types.

### SCD Type 1 – Overwrite

The existing record is updated with the latest value.

Before:

| emp_id | name | department |
| ------ | ---- | ---------- |
| 101    | Ravi | IT         |

After:

| emp_id | name | department |
| ------ | ---- | ---------- |
| 101    | Ravi | HR         |

The old value (IT) is lost.

SQL:

```
UPDATE employee
SET department = 'HR'
WHERE emp_id = 101;
```

### SCD Type 2 – Maintain complete history

Instead of overwriting the existing record, expire the old record and insert a new record.

| emp_id | name | department | start_date | end_date   | is_current |
| ------ | ---- | ---------- | ---------- | ---------- | ---------- |
| 101    | Ravi | IT         | 2025-01-01 | 2026-05-31 | false      |
| 101    | Ravi | HR         | 2026-06-01 | NULL       | true       |

SQL (conceptual implementation):

```
-- Expire old record
UPDATE employee_dim
SET end_date = CURRENT_DATE(),
    is_current = false
WHERE emp_id = 101
AND is_current = true;

-- Insert new record
INSERT INTO employee_dim
VALUES (
    101,
    'Ravi',
    'HR',
    CURRENT_DATE(),
    NULL,
    true
);
```

### SCD Type 3 – Maintain limited history

Store the current and previous values in separate columns.

| emp_id | current_dept | previous_dept |
| ------ | ------------ | ------------- |
| 101    | HR           | IT            |

Only limited history is maintained.

## 3. CDC vs SCD – Key differences

| Feature             | CDC                                    | SCD                         |
| ------------------- | -------------------------------------- | --------------------------- |
| Full form           | Change Data Capture                    | Slowly Changing Dimension   |
| Purpose             | Capture source changes                 | Manage dimension changes    |
| Focus               | Data ingestion                         | Data warehousing            |
| Identifies INSERT   | Yes                                    | Handles it                  |
| Identifies UPDATE   | Yes                                    | Handles it                  |
| Identifies DELETE   | Yes                                    | Can handle it               |
| Historical tracking | Depends on CDC mechanism and retention | Depends on SCD type         |
| Used in             | Bronze/Silver ingestion                | Silver/Gold transformations |
| Common tools        | Debezium, database logs, CDF           | SQL, PySpark, Delta MERGE   |

## 4. How CDC and SCD work together in a Databricks project

Consider a Salesforce-to-S3-to-Databricks pipeline using Medallion architecture.

Salesforce (Source)

INSERT / UPDATE / DELETE

CDC

Capture source changes

Bronze Layer

Store incoming CDC records

Silver Layer

Deduplication, validation, latest changes

SCD Type 1 / Type 2

Apply dimension change logic

Gold Layer

Current dimension or historical dimension tables

The important point:

- CDC tells you what changed in the source.
- SCD tells you how to maintain those changes in the target dimension table.

## 5. How to implement CDC + SCD using PySpark and Delta Lake

For example, suppose CDC provides the following records:

| emp_id | department | operation |
| ------ | ---------- | --------- |
| 101    | HR         | UPDATE    |
| 102    | Finance    | INSERT    |
| 103    | NULL       | DELETE    |

You can use Delta Lake MERGE for processing these changes.

SCD Type 1 example:

```
MERGE INTO employee_target t
USING employee_cdc s
ON t.emp_id = s.emp_id

WHEN MATCHED AND s.operation = 'DELETE'
THEN DELETE

WHEN MATCHED AND s.operation = 'UPDATE'
THEN UPDATE SET
    t.department = s.department

WHEN NOT MATCHED AND s.operation = 'INSERT'
THEN INSERT (
    emp_id,
    department
)
VALUES (
    s.emp_id,
    s.department
);
```

This is a simplified SCD Type 1 implementation using CDC records.

For SCD Type 2, the logic is different: you expire the existing record and insert a new version. A simple MERGE with an ordinary match condition is not sufficient to handle all Type 2 scenarios.

## 6. Interview question: Can we implement SCD without CDC?

Yes.

For example, if your source provides a complete daily snapshot, you can compare today's data with yesterday's data using primary keys and column comparisons.

You don't necessarily need CDC.

Similarly, can we use CDC without SCD?

Yes.

You can capture changes and simply update the target table using the latest records without maintaining historical versions.

## Remember this for interviews

CDC = What changed?

SCD = How do we store those changes?

Example:

- CDC detects Ravi's department changed from IT to HR.
- SCD Type 1 overwrites IT with HR.
- SCD Type 2 preserves IT and inserts HR as a new version.

My recommendation: For Databricks data engineering interviews, prepare CDC + SCD Type 1 + SCD Type 2 together, especially Delta Lake MERGE, handling duplicate CDC events, out-of-order events, and delete operations. These concepts are frequently connected in real-world incremental data pipelines.
