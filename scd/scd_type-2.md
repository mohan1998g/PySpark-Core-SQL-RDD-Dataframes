Below is a **complete, production‑grade master guide on Slowly Changing Dimension (SCD) Type‑2 in PySpark**, covering:

✅ Full theory  
✅ Data model & columns  
✅ End‑to‑end PySpark implementation  
✅ Multiple examples (basic → advanced)  
✅ Common interview problems + solutions  
✅ Real‑world variants  
✅ Performance considerations  
✅ TCS / enterprise interview focus points

This is **one of the most important data‑engineering topics**.

***

# ✅ SCD Type‑2 in PySpark — COMPLETE THEORY & EXAMPLES

***

## 🔹 1. What is a Slowly Changing Dimension (SCD)?

In Data Warehousing, **dimension tables** store descriptive attributes (customer, product, employee, etc.).

When these attributes change over time, we use **Slowly Changing Dimension (SCD)** techniques to handle them.

***

## 🔹 2. What is SCD Type‑2?

### ✅ Definition

**SCD Type‑2** tracks **full history** of changes by:

*   Creating a **new row** for each change
*   Marking old records as **expired**
*   Maintaining **effective date ranges**

👉 No data is overwritten.

***

## 🔹 3. Why SCD Type‑2 Is Needed

| Scenario                      | Why Type‑2              |
| ----------------------------- | ----------------------- |
| Customer address change       | Need historical billing |
| Employee department change    | Audit history           |
| Product price/category change | Trend analysis          |
| Compliance systems            | Regulatory requirement  |

***

## 🔹 4. SCD Type‑2 Table Structure (STANDARD)

Typical dimension table:

| Column                 | Purpose                         |
| ---------------------- | ------------------------------- |
| business\_key          | Natural key (cust\_id, emp\_id) |
| attributes             | Name, address, dept, etc.       |
| effective\_start\_date | When record became valid        |
| effective\_end\_date   | When record expired             |
| is\_current            | Current active version          |
| version (optional)     | Change sequence number          |

***

## 🔹 5. Sample Data

### 🔸 Incoming Source (Daily feed)

```text
cust_id | name  | city
1       | John  | Delhi
2       | Mary  | Mumbai
```

### 🔸 Existing Dimension Table

```text
cust_id | name | city  | start_dt | end_dt     | is_current
1       | John | Delhi | 2023-01-01 | 9999-12-31 | Y
2       | Mary | Pune  | 2023-01-01 | 9999-12-31 | Y
```

***

## 🔹 6. SCD Type‑2 Logic (High Level)

For each incoming record:

1.  🔍 Check if business key exists
2.  ✅ If new key → INSERT
3.  ✅ If exists but attributes changed:
    *   Expire old record
    *   Insert new record
4.  ✅ If no change → do nothing

***

# ✅ 7. BASIC SCD TYPE‑2 IMPLEMENTATION IN PYSPARK

***

## 🔹 Step 1: Create DataFrames

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window

source = spark.createDataFrame([
    (1, "John", "Delhi"),
    (2, "Mary", "Mumbai")
], ["cust_id", "name", "city"])

target = spark.createDataFrame([
    (1, "John", "Delhi", "2023-01-01", "9999-12-31", "Y"),
    (2, "Mary", "Pune",  "2023-01-01", "9999-12-31", "Y")
], ["cust_id", "name", "city", "start_dt", "end_dt", "is_current"])
```

***

## 🔹 Step 2: Join Source and Current Target

```python
joined = source.alias("src").join(
    target.filter("is_current='Y'").alias("tgt"),
    "cust_id",
    "left"
)
```

***

## 🔹 Step 3: Detect Changed Records

```python
changed = joined.filter(
    (col("tgt.cust_id").isNull()) | 
    (col("src.name") != col("tgt.name")) |
    (col("src.city") != col("tgt.city"))
)
```

***

## 🔹 Step 4: Expire Old Records

```python
expired = target.alias("tgt").join(
    changed.select("cust_id"), "cust_id"
).withColumn(
    "end_dt", current_date()
).withColumn(
    "is_current", lit("N")
)
```

***

## 🔹 Step 5: Insert New Version

```python
new_records = changed.select(
    col("src.cust_id"),
    col("src.name"),
    col("src.city")
).withColumn(
    "start_dt", current_date()
).withColumn(
    "end_dt", lit("9999-12-31")
).withColumn(
    "is_current", lit("Y")
)
```

***

## ✅ Final Result

`final_dimension = expired UNION ALL new_records + unchanged records`

***

# ✅ 8. USING WINDOW FUNCTIONS (Interview Favourite)

Sometimes input itself contains multiple versions.

***

### Example: Deduplicate latest record per key

```python
w = Window.partitionBy("cust_id").orderBy(col("update_ts").desc())

latest = source.withColumn(
    "rn", row_number().over(w)
).filter("rn = 1")
```

***

# ✅ 9. SCD TYPE‑2 INTERVIEW PROBLEMS & SOLUTIONS

***

## 🧠 Problem 1: Department Change

**Input**

```text
emp_id | dept
1      | HR
```

**Existing**

```text
emp_id | dept | is_current
1      | IT   | Y
```

✅ **Solution**

*   End IT record
*   Insert HR record

***

## 🧠 Problem 2: Ignore Non‑Changing Records

✅ Condition:

```python
(src.col1 != tgt.col1) OR ...
```

Only update when actual delta exists.

***

## 🧠 Problem 3: Multiple Changes Same Day

✅ Add `version` column:

```python
version = max(version) + 1
```

***

## 🧠 Problem 4: Initial Load (No Target)

✅ Just insert all source records with:

```text
start_dt = today
end_dt   = 9999-12-31
is_current = Y
```

***

## 🧠 Problem 5: Identify Current Records Only

✅ Query:

```sql
WHERE is_current = 'Y'
```

***

## 🧠 Problem 6: Point‑in‑Time Query

```sql
WHERE '2023-05-01' BETWEEN start_dt AND end_dt
```

***

# ✅ 10. Real‑World Variants

***

## ✅ Variant 1: Hash‑based Change Detection (Very Common)

```python
hash(col("name"), col("city"))
```

✅ Faster than column-by-column comparison

***

## ✅ Variant 2: Soft Deletes

Add column:

```text
is_deleted
```

***

## ✅ Variant 3: CDC‑based SCD2

Use:

*   Insert
*   Update
*   Delete flags from CDC systems

***

# ✅ 11. Performance Considerations (IMPORTANT)

✅ Use broadcast join if target is small  
✅ Partition target by business key  
✅ Avoid full table scan  
✅ Cache reused DataFrames  
✅ Prefer hash comparison  
✅ Avoid wide window partitions

***

# ✅ 12. SCD TYPE‑1 vs TYPE‑2 (INTERVIEW)

| Type   | Behavior               |
| ------ | ---------------------- |
| Type‑1 | Overwrite (no history) |
| Type‑2 | New row per change     |
| Type‑3 | Limited history        |
| Type‑4 | History table          |
| Type‑6 | Hybrid                 |

***

# ✅ 13. Interview‑Ready Explanation (Say This)

> “SCD Type‑2 captures full history by expiring old records and inserting a new version with effective dates. It’s widely used in data warehouses for auditability, trend analysis, and compliance.”

***

# ✅ 14. Common Mistakes (WATCH OUT)

❌ Not expiring old records  
❌ Multiple active records per key  
❌ Missing end dates  
❌ Comparing NULL incorrectly  
❌ Wrong join conditions

***

# ✅ 15. One‑Page Summary (Memorize)

✅ New key → INSERT  
✅ Changed attribute → EXPIRE + INSERT  
✅ Same record → DO NOTHING  
✅ Track start\_dt, end\_dt, is\_current  
✅ Use windows for latest record  
✅ Hash columns for performance

***

Just tell me 👍
