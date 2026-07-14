# ✅ 1. Real Company‑Level SCD Type‑2 Pipeline Design

This is how SCD2 is actually implemented in **banks, retail, telecom, and insurance clients**.

***

## 🔷 High‑Level Architecture

    Source Systems
     (CRM / ERP / OLTP)
            |
            |  (Daily / CDC / Streaming)
            v
    Landing Zone (Raw / Bronze)
            |
            |  (Cleansing, dedup)
            v
    Staging (Silver)
            |
            |  (SCD Type‑2 logic)
            v
    Dimension Table (Gold)
            |
            v
    Reporting / BI / Analytics

***

## 🔷 Pipeline Steps (Production‑Grade)

### ✅ Step 1: Source Ingestion

*   Full load or CDC feed
*   Schema validation
*   Basic null & datatype checks

```text
cust_id | name | city | update_ts
```

***

### ✅ Step 2: Staging / Deduplication

If multiple records per key arrive in one batch:

```python
w = Window.partitionBy("cust_id").orderBy(col("update_ts").desc())

stage_latest = source.withColumn(
    "rn", row_number().over(w)
).filter("rn = 1")
```

✅ Ensures **one record per key per batch**

***

### ✅ Step 3: Join with Current Dimension

```python
current_dim = dim.filter("is_current = 'Y'")

joined = stage_latest.alias("src").join(
    current_dim.alias("tgt"),
    "cust_id",
    "left"
)
```

***

### ✅ Step 4: Change Detection (Enterprise Pattern)

**Hash‑based comparison (common in real projects)**

```python
src_hash = xxhash64("src.name", "src.city")
tgt_hash = xxhash64("tgt.name", "tgt.city")
```

Change condition:

*   New key OR hash mismatch

***

### ✅ Step 5: Expire Old Records

```python
expired = current_dim.join(changed_keys, "cust_id") \
    .withColumn("end_date", current_date()) \
    .withColumn("is_current", lit("N"))
```

***

### ✅ Step 6: Insert New Records

```python
new_rows = stage_latest.select(...) \
    .withColumn("start_date", current_date()) \
    .withColumn("end_date", lit("9999-12-31")) \
    .withColumn("is_current", lit("Y"))
```

***

### ✅ Step 7: Atomic Write

✅ In production:

*   Use **Delta Lake MERGE**
*   Or transactional overwrite

***

## 🔷 Enterprise Design Rules

✅ Exactly **one active row per business key**  
✅ Historical records never updated  
✅ Surrogate key used for joins  
✅ Partition by business key / date  
✅ Late arrivals handled carefully

***

# ✅ 2. Mock SCD Type‑2 Coding Interview (Step‑by‑Step)

This is **almost identical** to what TCS asks.

***

## 🧪 Interview Problem

### Source Table

```text
cust_id | name | city
1       | John | Delhi
2       | Mary | Mumbai
```

### Target Dimension

```text
cust_id | name | city | start_dt | end_dt     | is_current
1       | John | Delhi| 2023-01-01| 9999-12-31 | Y
2       | Mary | Pune | 2023-01-01| 9999-12-31 | Y
```

***

## ✅ Expected Output

*   No change for customer **1**
*   City change for customer **2**
    *   Old Pune record expired
    *   New Mumbai record inserted

***

## ✅ Step‑by‑Step Solution

### Step 1: Join Source with Current Records

```python
joined = src.join(tgt.filter("is_current='Y'"), "cust_id", "left")
```

***

### Step 2: Identify Changed Records

```python
changed = joined.filter(
    col("tgt.cust_id").isNull() |
    (col("src.name") != col("tgt.name")) |
    (col("src.city") != col("tgt.city"))
)
```

***

### Step 3: Expire Old Rows

```python
expired = tgt.join(changed.select("cust_id"), "cust_id") \
    .withColumn("end_dt", current_date()) \
    .withColumn("is_current", lit("N"))
```

***

### Step 4: Insert New Rows

```python
new_rows = changed.select("src.cust_id", "src.name", "src.city") \
    .withColumn("start_dt", current_date()) \
    .withColumn("end_dt", lit("9999-12-31")) \
    .withColumn("is_current", lit("Y"))
```

***

### ✅ Interview Explanation (Say This)

> “I first join source with the active dimension records, detect changes, expire old rows by updating end\_date and is\_current flag, and insert a new record with updated attributes and a new effective date.”

✅ This answer alone **clears the round**.

***

# ✅ 3. SQL vs PySpark SCD Type‑2 Comparison

This is **frequently asked verbally**.

***

## 🔷 SQL‑Based SCD Type‑2

### Example (Conceptual SQL)

```sql
-- Expire old records
UPDATE dim
SET end_dt = current_date,
    is_current = 'N'
WHERE cust_id IN (changed_keys);

-- Insert new records
INSERT INTO dim
SELECT cust_id, name, city, current_date, '9999-12-31', 'Y'
FROM source;
```

### ✅ Pros

*   Simple logic
*   Good for small data
*   Easier to read

### ❌ Cons

*   Limited scalability
*   Hard with CDC
*   Performance issues on big tables

***

## 🔷 PySpark SCD Type‑2

### ✅ Pros

*   Distributed processing
*   Scales to billions of records
*   Handles CDC & streaming
*   Flexible transformations
*   Better for cloud data lakes

### ❌ Cons

*   More code
*   Requires careful design

***

## 🔥 Interview Comparison Table

| Aspect         | SQL       | PySpark     |
| -------------- | --------- | ----------- |
| Scalability    | ❌ Limited | ✅ Massive   |
| CDC handling   | ❌ Hard    | ✅ Easy      |
| Performance    | ❌ Slower  | ✅ Optimized |
| Complexity     | ✅ Simple  | ⚠️ Moderate |
| Enterprise use | ⚠️ Medium | ✅ High      |

***

## ✅ Interview‑Perfect Answer

> “SQL SCD2 works well for small warehouse tables. For enterprise‑scale pipelines with CDC and large volumes, PySpark provides better scalability, flexibility, and performance.”

***

# ✅ Common Follow‑Up Questions & Answers

### ❓ How do you ensure one active record?

✅ Unique constraint or validation on `is_current = 'Y'`

***

### ❓ How do you handle late‑arriving data?

✅ Compare based on business effective date, not load date

***

### ❓ How do you optimize SCD2?

✅ Hash comparison, broadcast joins, partitioning

***

# ✅ Final Power Statement (Memorize This)

> “SCD Type‑2 preserves full historical dimension changes by expiring old records and inserting new versions with effective dates. In enterprise pipelines, PySpark is preferred due to scalability, CDC handling, and integration with modern data lakes.”

***

If you want next (very powerful):
✅ **Delta Lake MERGE‑based SCD2**
✅ **Streaming SCD2 using Structured Streaming**
✅ **End‑to‑end project mock for resume**
✅ **TCS SCD2 whiteboard questions**

Just tell me 👍
