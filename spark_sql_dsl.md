# 📌 1. Creating DataFrames

```python
data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]
df = spark.createDataFrame(data, ["id","tdate","amount","category","product","spendby"])
df.createOrReplaceTempView("df")
```

***

# 📌 2. Basic Select Queries

```sql
SELECT * FROM df;
SELECT id, tdate FROM df ORDER BY id;
```

```python
df.select("*").show()
df.select("id","tdate").orderBy("id").show()
```

***

# 📌 3. Filtering Data

### ✅ WHERE Conditions

```sql
SELECT * FROM df WHERE category='Exercise';
SELECT * FROM df WHERE category='Exercise' AND spendby='cash';
```

```python
df.filter("category='Exercise'").show()
df.where("category='Exercise' AND spendby='cash'").show()
```

***

### ✅ IN / NOT IN

```sql
SELECT * FROM df WHERE category IN ('Exercise','Gymnastics');
SELECT * FROM df WHERE category NOT IN ('Exercise','Gymnastics');
```

***

### ✅ LIKE

```sql
SELECT * FROM df WHERE product LIKE '%Gymnastics%';
```

***

### ✅ NULL Handling

```sql
SELECT * FROM df WHERE product IS NULL;
```

***

# 📌 4. Aggregations

```sql
SELECT MAX(id) FROM df;
SELECT MIN(id) FROM df;
SELECT COUNT(1) FROM df;
```

```python
df.agg(max("id")).show()
df.agg(count("id")).show()
```

***

# 📌 5. Conditional Column (CASE WHEN)

```sql
SELECT *, CASE WHEN spendby='cash' THEN 1 ELSE 0 END AS status FROM df;
```

```python
df.withColumn("status", when(col("spendby")=="cash",1).otherwise(0)).show()
```

***

# 📌 6. String Functions

### ✅ CONCAT

```sql
SELECT CONCAT(id,'-',category) FROM df;
SELECT CONCAT_WS('-',id,category,product) FROM df;
```

***

### ✅ LOWER

```sql
SELECT LOWER(category) FROM df;
```

***

### ✅ TRIM

```sql
SELECT TRIM(product) FROM df;
```

***

### ✅ SUBSTRING

```sql
SELECT SUBSTRING(TRIM(product),1,10) FROM df;
```

***

### ✅ SPLIT (Add-on)

```python
from pyspark.sql.functions import split

df.withColumn("split_category", split(col("category"), " ")).show()
```

***

### ✅ SUBSTRING\_INDEX

```sql
SELECT SUBSTRING_INDEX(category,' ',1) FROM df;
```

***

# 📌 7. Numeric Functions

```sql
SELECT CEIL(amount) FROM df;
SELECT ROUND(amount) FROM df;
```

***

# 📌 8. Null Handling

```sql
SELECT COALESCE(product,'NA') FROM df;
```

***

# 📌 9. DISTINCT

```sql
SELECT DISTINCT category, spendby FROM df;
```

***

# 📌 10. UNION Operations

### ✅ UNION ALL (keeps duplicates)

```sql
SELECT * FROM df
UNION ALL
SELECT * FROM df1;
```

### ✅ UNION (removes duplicates)

```sql
SELECT * FROM df
UNION
SELECT * FROM df1;
```

***

# 📌 11. GROUP BY Operations

### ✅ SUM

```sql
SELECT category, SUM(amount) FROM df GROUP BY category;
```

***

### ✅ Multiple Grouping

```sql
SELECT category, spendby, SUM(amount)
FROM df
GROUP BY category, spendby;
```

***

### ✅ COUNT + SUM

```sql
SELECT category, COUNT(*), SUM(amount)
FROM df
GROUP BY category;
```

***

# 📌 12. ORDER BY

```sql
SELECT category, MAX(amount)
FROM df
GROUP BY category
ORDER BY category DESC;
```

***

# 📌 13. Window Functions

```python
from pyspark.sql.window import Window
window = Window.partitionBy("category").orderBy(col("amount").desc())
```

***

### ✅ Row Number

```sql
ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC)
```

***

### ✅ Rank

```sql
RANK() OVER (PARTITION BY category ORDER BY amount DESC)
```

***

### ✅ Dense Rank

```sql
DENSE_RANK() OVER (...)
```

***

### ✅ Lead & Lag

```sql
LEAD(amount) OVER (...)
LAG(amount, 2) OVER (...)
```

***

# 📌 14. HAVING Clause

```sql
SELECT category, COUNT(*)
FROM df
GROUP BY category
HAVING COUNT(*) > 1;
```

***

# 📌 15. Joins

### ✅ INNER JOIN

```sql
SELECT a.id, a.name, b.product
FROM cust a
JOIN prod b ON a.id = b.id;
```

***

### ✅ LEFT JOIN

```sql
SELECT a.id, a.name, b.product
FROM cust a
LEFT JOIN prod b ON a.id=b.id;
```

***

### ✅ RIGHT JOIN

```sql
SELECT a.id, a.name, b.product
FROM cust a
RIGHT JOIN prod b ON a.id=b.id;
```

***

### ✅ FULL JOIN

```sql
SELECT a.id, a.name, b.product
FROM cust a
FULL JOIN prod b ON a.id=b.id;
```

***

### ✅ LEFT ANTI JOIN

```sql
SELECT a.id,a.name
FROM cust a
LEFT ANTI JOIN prod b ON a.id=b.id;
```

***

### ✅ LEFT SEMI JOIN

```sql
SELECT a.id,a.name
FROM cust a
LEFT SEMI JOIN prod b ON a.id=b.id;
```

***

# 📌 16. Date Conversion

```sql
SELECT id, tdate,
FROM_UNIXTIME(UNIX_TIMESTAMP(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') AS con_date
FROM df;
```

***

# 📌 17. Aggregation with Converted Date

```sql
SELECT SUM(amount) AS total, con_date
FROM (
    SELECT id, tdate,
    FROM_UNIXTIME(UNIX_TIMESTAMP(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') AS con_date,
    amount
    FROM df
)
GROUP BY con_date;
```

***

# ✅ Final Notes

* Spark SQL and DataFrame APIs provide equivalent operations
* SQL is easier for querying
* DataFrame is better for programmatic transformations
* Window functions are very important for interviews

***

✅ If you want, I can:

* Convert this into a **downloadable `.md` file**
* Add **expected outputs for each query**
* Create **interview Q\&A from this**

Just tell me 👍
