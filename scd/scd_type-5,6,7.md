# SCD Type 5

## Definition

SCD Type 5 = **Type 1 + Type 4**

It combines:

* **Type 1** → Current value stored in Dimension table
* **Type 4** → Historical records stored in separate History table

This provides:

✅ Fast access to current data

✅ Complete history available separately

***

# Business Scenario

Customer changes city multiple times:

```text
Hyderabad → Bangalore → Chennai
```

***

## Current Customer Dimension

### customer\_dim

| Customer\_ID | Customer\_Name | Current\_City |
| ------------ | -------------- | ------------- |
| 101          | Mohan          | Chennai       |

Only latest value is maintained.

***

## History Table

### customer\_history

| Customer\_ID | City      | Change\_Date |
| ------------ | --------- | ------------ |
| 101          | Hyderabad | 2023-01-01   |
| 101          | Bangalore | 2024-01-01   |
| 101          | Chennai   | 2025-01-01   |

Complete history maintained.

***

# How Update Happens

Suppose current city:

```text
Bangalore
```

Source sends:

```text
Chennai
```

### Step 1

Copy old record to history table.

```sql
INSERT INTO customer_history
SELECT
customer_id,
current_city,
CURRENT_DATE
FROM customer_dim
WHERE customer_id=101;
```

***

### Step 2

Update current table.

```sql
UPDATE customer_dim
SET current_city='Chennai'
WHERE customer_id=101;
```

***

# PySpark Example

```python
old_record = target_df.filter(
    "customer_id = 101"
)

old_record.write.mode("append") \
    .saveAsTable("customer_history")
```

Update current dimension:

```python
updated_df = target_df.withColumn(
    "current_city",
    when(
        col("customer_id")==101,
        "Chennai"
    ).otherwise(col("current_city"))
)
```

***

# Architecture Diagram

```text
Source
   |
   ▼
Customer Dimension
(Current Data)

101 | Chennai

   |
   ▼

Customer History

101 | Hyderabad
101 | Bangalore
```

***

# When Type 5 is Used

### Example 1

Customer Profile System

Current Address:

```text
Bangalore
```

History kept separately.

***

### Example 2

Telecom Subscriber

Current Plan:

```text
Premium
```

Previous plans stored in history table.

***

### Example 3

Bank Account

Current Branch

Historical branch movements stored separately.

***

# Advantages

✅ Fast reporting

✅ Smaller dimension table

✅ History available

✅ Easy maintenance

***

# Disadvantages

❌ Requires two tables

❌ More ETL logic

***

# SCD Type 6

## Definition

Type 6 is called:

```text
Hybrid SCD
```

or

```text
Type 1 + Type 2 + Type 3
```

It combines:

### Type 1

Overwrite current values

### Type 2

Maintain full history using new rows

### Type 3

Store previous value

***

# Why Type 6 Exists

Business wants:

1. Current value
2. Previous value
3. Complete historical records

All together.

***

# Customer Example

Customer moved:

```text
Hyderabad
     ↓
Bangalore
     ↓
Chennai
```

***

# Before Change

| SK | Cust\_ID | Current\_City | Previous\_City | Current\_Flag |
| -- | -------- | ------------- | -------------- | ------------- |
| 1  | 101      | Hyderabad     | NULL           | Y             |

***

# After Bangalore Move

| SK | Cust\_ID | Current\_City | Previous\_City | Current\_Flag |
| -- | -------- | ------------- | -------------- | ------------- |
| 1  | 101      | Hyderabad     | NULL           | N             |
| 2  | 101      | Bangalore     | Hyderabad      | Y             |

***

# After Chennai Move

| SK | Cust\_ID | Current\_City | Previous\_City | Current\_Flag |
| -- | -------- | ------------- | -------------- | ------------- |
| 1  | 101      | Hyderabad     | NULL           | N             |
| 2  | 101      | Bangalore     | Hyderabad      | N             |
| 3  | 101      | Chennai       | Bangalore      | Y             |

***

## Notice

We now have:

### Type 2

Multiple rows

```text
Hyderabad
Bangalore
Chennai
```

***

### Type 3

Previous city retained

```text
Previous_City
```

***

### Type 1

Current row always reflects latest state.

***

# SQL Example

## Expire old row

```sql
UPDATE customer_dim
SET current_flag='N',
    expiry_date=CURRENT_DATE
WHERE customer_id=101
AND current_flag='Y';
```

***

## Insert new row

```sql
INSERT INTO customer_dim
(
customer_id,
current_city,
previous_city,
effective_date,
expiry_date,
current_flag
)
VALUES
(
101,
'Chennai',
'Bangalore',
CURRENT_DATE,
'9999-12-31',
'Y'
);
```

***

# Databricks / Delta Lake Example

```python
from delta.tables import DeltaTable
```

Existing:

```text
101 Hyderabad
```

Incoming:

```text
101 Bangalore
```

Pseudo Logic:

```python
deltaTable.alias("t") \
.merge(
source_df.alias("s"),
"t.customer_id=s.customer_id \
 AND t.current_flag='Y'"
) \
.whenMatchedUpdate(
set={
"current_flag":"'N'"
}
) \
.execute()
```

Insert new version.

```python
new_df = source_df \
.withColumn("previous_city",
            lit("Hyderabad")) \
.withColumn("current_flag",
            lit("Y"))
```

***

# Real Industry Examples

### Employee Department Tracking

```text
IT → HR → Finance
```

Need full history and previous department.

***

### Customer Segment

```text
Silver → Gold → Platinum
```

Track all transitions.

***

### Product Pricing

```text
1000 → 1200 → 1500
```

Maintain historical pricing.

***

# Advantages

✅ Full history available

✅ Previous value available

✅ Current value available

✅ Excellent for analytics

***

# Disadvantages

❌ Higher storage

❌ Complex ETL

***

# SCD Type 7

## Definition

Type 7 = Dual relationship model

Supports both:

```text
Current Reporting
```

and

```text
Historical Reporting
```

simultaneously.

***

# Key Concept

Uses two keys:

### Natural Key

```text
Customer_ID
```

Business key.

***

### Surrogate Key

```text
Customer_SK
```

Warehouse-generated key.

***

# Example

Customer History:

| SK | Cust\_ID | City      |
| -- | -------- | --------- |
| 1  | 101      | Hyderabad |
| 2  | 101      | Bangalore |
| 3  | 101      | Chennai   |

***

# Fact Table

Instead of storing only one key,

Fact table stores:

| Order\_ID | Customer\_SK | Customer\_ID |
| --------- | ------------ | ------------ |
| 5001      | 1            | 101          |
| 5002      | 2            | 101          |
| 5003      | 3            | 101          |

***

# Why Store Both?

To answer two different questions.

***

# Historical Analysis

Question:

```text
Show sales by customer city at the
time the sale occurred.
```

Join on:

```sql
Customer_SK
```

```sql
SELECT *
FROM fact_sales f
JOIN customer_dim d
ON f.customer_sk=d.customer_sk;
```

Result:

```text
Sale 1 → Hyderabad
Sale 2 → Bangalore
Sale 3 → Chennai
```

Historical accuracy maintained.

***

# Current Analysis

Question:

```text
Show all sales according to
customer's current city.
```

Join on:

```sql
Customer_ID
```

Current Row:

```text
Customer_ID=101
Current City=Chennai
```

Query:

```sql
SELECT *
FROM fact_sales f
JOIN customer_dim d
ON f.customer_id=d.customer_id
AND d.current_flag='Y';
```

Result:

All sales appear under:

```text
Chennai
```

***

# Visual Explanation

### Dimension Table

```text
SK   Cust_ID   City

1    101       Hyderabad
2    101       Bangalore
3    101       Chennai
```

### Fact Table

```text
Order   SK   Cust_ID

5001    1     101
5002    2     101
5003    3     101
```

***

# Type 7 Reporting Capability

## Historical View

```text
5001 Hyderabad
5002 Bangalore
5003 Chennai
```

***

## Current View

```text
5001 Chennai
5002 Chennai
5003 Chennai
```

Same data, different business perspectives.

***

# Type 5 vs Type 6 vs Type 7

| Feature                        | Type 5             | Type 6     | Type 7     |
| ------------------------------ | ------------------ | ---------- | ---------- |
| Current Data                   | Yes                | Yes        | Yes        |
| Historical Data                | Separate Table     | Same Table | Same Table |
| Previous Value                 | No                 | Yes        | No         |
| Multiple Rows                  | No (Current Table) | Yes        | Yes        |
| Uses History Table             | Yes                | No         | No         |
| Uses Surrogate Key             | Optional           | Yes        | Yes        |
| Current + Historical Reporting | Partial            | Good       | Excellent  |
| Complexity                     | Medium             | High       | Very High  |

***

# Interview Shortcut

If interviewer asks:

### "Need current data + complete history separately?"

✅ **Type 5**

***

### "Need current value + previous value + full history?"

✅ **Type 6**

***

### "Need both historical and current reporting from same fact table?"

✅ **Type 7**

***

### Most Important Real-World Usage

**Type 2 (80%) > Type 6 (15%) > Type 1 (5%)**

Type 5 and Type 7 are comparatively rare, usually seen in large enterprise data warehouses where reporting requirements are very sophisticated.
