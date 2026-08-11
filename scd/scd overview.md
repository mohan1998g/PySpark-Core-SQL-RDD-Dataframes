## What is SCD?

**Slowly Changing Dimension (SCD)** is a data warehousing concept used to manage and track changes in dimension tables over time.

### Example Dimension Table: Customer

| Customer\_ID | Customer\_Name | City      |
| ------------ | -------------- | --------- |
| 101          | Mohan          | Hyderabad |

Suppose Mohan moves from **Hyderabad** to **Bangalore**.

Business may want:

1. Update with latest city only?
2. Keep old and new city history?
3. Keep limited history?
4. Track effective dates?

This is where SCD Types come into play.

***

# SCD Type 0 – Fixed Dimension (No Changes Allowed)

## Definition

Once data is inserted, it never changes.

## Use Case

Fields that should never be modified:

* Date of Birth
* PAN Number
* Customer Registration Date
* Original Joining Date

***

## Example

### Initial Record

| Customer\_ID | Name  | DOB         |
| ------------ | ----- | ----------- |
| 101          | Mohan | 01-Jan-1990 |

Changed DOB comes from source:

| Customer\_ID | Name  | DOB         |
| ------------ | ----- | ----------- |
| 101          | Mohan | 15-Jan-1990 |

### Result

Ignore the update.

| Customer\_ID | Name  | DOB         |
| ------------ | ----- | ----------- |
| 101          | Mohan | 01-Jan-1990 |

***

## SQL Example

```sql
-- Ignore incoming updates
INSERT INTO customer_dim
SELECT *
FROM staging_customer s
WHERE NOT EXISTS (
    SELECT 1
    FROM customer_dim d
    WHERE d.customer_id = s.customer_id
);
```

***

## PySpark Example

```python
final_df = target_df.union(
    source_df.join(target_df, "customer_id", "left_anti")
)
```

***

## Real-world Examples

1. DOB
2. PAN Number
3. Passport Number
4. Registration Date
5. Employee Joining Date
6. Product Launch Date
7. Vehicle Chassis Number
8. Aadhaar Number
9. First Order Date
10. Initial Customer Sign-up Date

***

# SCD Type 1 – Overwrite

## Definition

Old value is replaced with new value.

No history maintained.

***

## Example

### Before

| ID  | Name  | City      |
| --- | ----- | --------- |
| 101 | Mohan | Hyderabad |

### Source

| ID  | Name  | City      |
| --- | ----- | --------- |
| 101 | Mohan | Bangalore |

### After

| ID  | Name  | City      |
| --- | ----- | --------- |
| 101 | Mohan | Bangalore |

History lost.

***

## SQL Example

```sql
UPDATE customer_dim
SET city = 'Bangalore'
WHERE customer_id = 101;
```

***

## MERGE Example

```sql
MERGE INTO customer_dim d
USING staging_customer s
ON d.customer_id=s.customer_id
WHEN MATCHED THEN
UPDATE SET d.city=s.city
WHEN NOT MATCHED THEN
INSERT VALUES
(s.customer_id,s.name,s.city);
```

***

## PySpark Example

```python
merged_df = source_df.alias("s").join(
    target_df.alias("t"),
    "customer_id",
    "full"
).selectExpr(
    "customer_id",
    "coalesce(s.name,t.name) as name",
    "coalesce(s.city,t.city) as city"
)
```

***

## Real-world Examples

1. Customer email correction
2. Employee phone number
3. Product description
4. Supplier contact information
5. Address typo correction
6. Color spelling correction
7. State code correction
8. Customer nickname
9. User profile picture
10. Temporary attribute updates

***

# SCD Type 2 – Full Historical Tracking

## Definition

Create a new row whenever a change occurs.

Most commonly used in Data Warehousing.

***

## Additional Columns

```text
surrogate_key
effective_date
expiry_date
current_flag
```

***

## Example

### Before

| SK | Cust\_ID | Name  | City      | Current |
| -- | -------- | ----- | --------- | ------- |
| 1  | 101      | Mohan | Hyderabad | Y       |

Customer moved to Bangalore.

### After

| SK | Cust\_ID | Name  | City      | Current |
| -- | -------- | ----- | --------- | ------- |
| 1  | 101      | Mohan | Hyderabad | N       |
| 2  | 101      | Mohan | Bangalore | Y       |

***

## SQL Example

### Step 1 Expire Old Record

```sql
UPDATE customer_dim
SET current_flag='N',
    expiry_date=CURRENT_DATE
WHERE customer_id=101
AND current_flag='Y';
```

### Step 2 Insert New Record

```sql
INSERT INTO customer_dim
(
customer_id,
name,
city,
effective_date,
expiry_date,
current_flag
)
VALUES
(
101,
'Mohan',
'Bangalore',
CURRENT_DATE,
'9999-12-31',
'Y'
);
```

***

## PySpark Example

```python
from pyspark.sql.functions import lit, current_date

expired_df = target_df.withColumn(
    "current_flag",
    lit("N")
)

new_df = source_df.withColumn(
    "current_flag",
    lit("Y")
).withColumn(
    "effective_date",
    current_date()
)

final_df = expired_df.union(new_df)
```

***

## Real-world Examples

1. Customer address history
2. Employee department history
3. Employee manager history
4. Product price history
5. Branch location changes
6. Sales territory changes
7. Insurance plan history
8. Customer marital status history
9. Vendor region history
10. Credit score tracking

***

# SCD Type 3 – Limited History

## Definition

Store current value and previous value in the same row.

Only limited history retained.

***

## Example

### Before

| ID  | City      |
| --- | --------- |
| 101 | Hyderabad |

### After

| ID  | Previous\_City | Current\_City |
| --- | -------------- | ------------- |
| 101 | Hyderabad      | Bangalore     |

***

## SQL Example

```sql
UPDATE customer_dim
SET previous_city = current_city,
    current_city = 'Bangalore'
WHERE customer_id = 101;
```

***

## PySpark Example

```python
from pyspark.sql.functions import col

updated_df = target_df.withColumn(
    "previous_city",
    col("current_city")
).withColumn(
    "current_city",
    lit("Bangalore")
)
```

***

## Real-world Examples

1. Previous address
2. Previous manager
3. Previous designation
4. Previous account type
5. Previous mobile number
6. Previous branch
7. Previous city
8. Previous vendor
9. Previous tax slab
10. Last known customer status

***

# SCD Type 4 – History Table

## Definition

Current data stored in dimension table.

Historical data stored in separate history table.

***

## Structure

### Current Table

```text
customer_dim
```

### History Table

```text
customer_history
```

***

## Example

### Current Table

| Cust\_ID | City      |
| -------- | --------- |
| 101      | Bangalore |

### History Table

| Cust\_ID | City      |
| -------- | --------- |
| 101      | Hyderabad |

***

## SQL Example

```sql
INSERT INTO customer_history
SELECT *
FROM customer_dim
WHERE customer_id=101;

UPDATE customer_dim
SET city='Bangalore'
WHERE customer_id=101;
```

***

## PySpark Example

```python
history_df = target_df.filter(
    "customer_id = 101"
)

history_df.write.mode("append").saveAsTable(
    "customer_history"
)
```

***

## Real-world Examples

1. Banking customer changes
2. Insurance records
3. Telecom customer history
4. Product pricing history
5. Employee movement history
6. Medical records
7. Membership changes
8. Subscription plans
9. Vendor changes
10. Account ownership changes

***

# SCD Type 5

## Definition

Combination of:

* Type 1
* Type 4

Current values in dimension table and full history separately maintained.

***

## Example

### Current Table

| ID  | City      |
| --- | --------- |
| 101 | Bangalore |

### History Table

| ID  | Old City  |
| --- | --------- |
| 101 | Hyderabad |

***

## Use Cases

1. CRM systems
2. Telecom customers
3. Financial products
4. Insurance plans
5. Retail customer profiles
6. Loyalty programs
7. Subscription services
8. Vendor management
9. Banking applications
10. Healthcare member information

***

# SCD Type 6 (Hybrid SCD)

## Definition

Combination of:

* Type 1
* Type 2
* Type 3

Most powerful and commonly asked in interviews.

***

## Structure

| Cust\_ID | Current\_City | Previous\_City | Effective\_Date | Current\_Flag |
| -------- | ------------- | -------------- | --------------- | ------------- |

***

## Example

Before:

| Cust\_ID | City      |
| -------- | --------- |
| 101      | Hyderabad |

After:

| Cust\_ID | Current\_City | Previous\_City |
| -------- | ------------- | -------------- |
| 101      | Bangalore     | Hyderabad      |

And Type 2 history maintained.

***

## SQL Example

```sql
UPDATE customer_dim
SET current_flag='N'
WHERE customer_id=101
AND current_flag='Y';

INSERT INTO customer_dim
(
customer_id,
current_city,
previous_city,
effective_date,
current_flag
)
VALUES
(
101,
'Bangalore',
'Hyderabad',
CURRENT_DATE,
'Y'
);
```

***

## PySpark Example

```python
new_df = source_df.withColumn(
    "previous_city",
    lit("Hyderabad")
).withColumn(
    "current_flag",
    lit("Y")
)
```

***

## Real-world Examples

1. Employee hierarchy tracking
2. Banking customers
3. Telecom subscribers
4. Insurance plans
5. Product pricing
6. Sales territories
7. Customer segmentation
8. Healthcare records
9. Vendor assignments
10. Branch management

***

# SCD Type 7

## Definition

Supports both:

* Current view
* Historical view

Uses:

* Natural Key
* Surrogate Key

simultaneously.

***

## Example

### Current Query

```sql
SELECT *
FROM customer_dim
WHERE customer_id=101;
```

### Historical Query

```sql
SELECT *
FROM customer_dim
WHERE surrogate_key=5;
```

***

## Real-world Examples

1. Enterprise Data Warehouse
2. Banking systems
3. SAP BW
4. Customer analytics
5. Regulatory reporting
6. Insurance analytics
7. Retail analytics
8. Telecom warehouses
9. Healthcare analytics
10. Audit reporting

***

# Interview Comparison Table

| Type   | History             | Rows              | Common Usage        |
| ------ | ------------------- | ----------------- | ------------------- |
| Type 0 | No change           | Same              | Fixed data          |
| Type 1 | No                  | Update            | Current value       |
| Type 2 | Full                | New row           | Historical tracking |
| Type 3 | Limited             | Same row          | Previous value      |
| Type 4 | Separate table      | Current + History | Large history       |
| Type 5 | Type1 + Type4       | Hybrid            | Current + History   |
| Type 6 | Type1+2+3           | Hybrid            | Advanced DW         |
| Type 7 | Natural + Surrogate | Hybrid            | Dual reporting      |

***

# Most Important Interview Questions

### 1. Which SCD type is most commonly used?

✅ **Type 2**

***

### 2. Which SCD preserves complete history?

✅ **Type 2**

***

### 3. Which SCD overwrites history?

✅ **Type 1**

***

### 4. Which SCD keeps only previous value?

✅ **Type 3**

***

### 5. Which SCD uses separate history table?

✅ **Type 4**

***

### 6. Which SCD combines Type 1, 2, and 3?

✅ **Type 6**

***

### 7. Which key is required in Type 2?

✅ **Surrogate Key**

***

### 8. Common columns in SCD Type 2?

```text
surrogate_key
effective_date
expiry_date
current_flag
```

***

### 9. Best SCD for auditing?

✅ Type 2 / Type 6

***

### 10. Most asked SCD in Databricks & PySpark interviews?

✅ SCD Type 1 and SCD Type 2 using MERGE INTO (Delta Lake)

```python
deltaTable.alias("t").merge(
    source_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    set={"city": "s.city"}
).whenNotMatchedInsertAll().execute()
```

For SQL/ETL/Data Engineer interviews, focus heavily on **Type 1 and Type 2 implementations in SQL, PySpark, and Delta Lake**, because they account for the majority of real-world warehouse scenarios.
