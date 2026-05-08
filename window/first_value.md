Below is a **complete, end‑to‑end explanation of the `first_value` window function in PySpark**, written in a **theory → behavior → syntax → examples → edge cases → interview notes** format.

This is exactly the depth expected in **TCS / enterprise Spark interviews**.

***

# ✅ `first_value` Window Function in PySpark — Complete Theory & Examples

***

## 1️⃣ What is a Window Function?

In Spark SQL / PySpark, **window functions** perform calculations across a **set of rows related to the current row**, without collapsing rows (unlike `groupBy`).

👉 Each row **retains its identity**, but can see other rows in its *window*.

***

## 2️⃣ What is `first_value`?

### ✅ Definition

`first_value` is a **window function** that returns the **first value in an ordered window partition**.

> ✅ It returns the value from the **first row in the window frame**, based on the `ORDER BY`.

***

## 3️⃣ Key Idea Behind `first_value`

*   Data is divided using **PARTITION BY**
*   Rows inside each partition are **ordered**
*   A window frame is applied
*   `first_value` picks the **first row’s value in that frame**

***

## 4️⃣ Syntax in PySpark

```python
from pyspark.sql.functions import first_value
from pyspark.sql.window import Window
```

```python
first_value(column).over(window_spec)
```

***

## 5️⃣ Basic Example Dataset

```python
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 120),
    ("A", "2024-01-03", 110),
    ("B", "2024-01-01", 200),
    ("B", "2024-01-02", 220),
]
```

```python
df = spark.createDataFrame(data, ["dept", "date", "sales"])
```

***

## 6️⃣ Simple `first_value` Example

### Goal:

👉 Find **first sales value per department**, ordered by date

### Window Spec:

```python
from pyspark.sql.window import Window

w = Window.partitionBy("dept").orderBy("date")
```

### Code:

```python
from pyspark.sql.functions import first_value

df.withColumn(
    "first_sales",
    first_value("sales").over(w)
).show()
```

### ✅ Output:

```text
+----+----------+-----+-----------+
|dept|date      |sales|first_sales|
+----+----------+-----+-----------+
|A   |2024-01-01|100  |100        |
|A   |2024-01-02|120  |100        |
|A   |2024-01-03|110  |100        |
|B   |2024-01-01|200  |200        |
|B   |2024-01-02|220  |200        |
+----+----------+-----+-----------+
```

✅ Explanation:

*   Partition by `dept`
*   Order by `date`
*   Take first row’s `sales` in each partition

***

## 7️⃣ Important: Window Frame Matters (VERY IMPORTANT)

### Default Window Frame (⚠️ Trap)

When **ORDER BY is present**, Spark uses:

```text
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

This means:

*   Frame **ends at current row**
*   Works fine for `first_value`
*   ❌ Dangerous for functions like `last_value`

***

## 8️⃣ Explicit Frame (Best Practice)

Always **define frame explicitly**.

```python
w = (
    Window
    .partitionBy("dept")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
```

```python
df.withColumn(
    "first_sales",
    first_value("sales").over(w)
).show()
```

✅ Output is the same, but **now guaranteed correct in all cases**.

***

## 9️⃣ `first_value` vs `min`

| Aspect            | first\_value | min  |
| ----------------- | ------------ | ---- |
| Respects order    | ✅ Yes        | ❌ No |
| Requires ORDER BY | ✅ Yes        | ❌ No |
| Returns first row | ✅ Yes        | ❌ No |

Example difference:

```python
df.groupBy("dept").agg(min("sales"))
```

❌ This gives minimum sales, NOT first sale chronologically.

***

## 🔥 10️⃣ first\_value with DESC Order (Common Use Case)

### Latest value treated as “first”

```python
w = Window.partitionBy("dept").orderBy(df.date.desc())
```

```python
df.withColumn(
    "latest_sales",
    first_value("sales").over(w)
).show()
```

✅ Output:

```text
+----+----------+-----+-------------+
|dept|date      |sales|latest_sales |
+----+----------+-----+-------------+
|A   |2024-01-01|100  |110          |
|A   |2024-01-02|120  |110          |
|A   |2024-01-03|110  |110          |
|B   |2024-01-01|200  |220          |
|B   |2024-01-02|220  |220          |
+----+----------+-----+-------------+
```

✅ Trick:

*   DESC order + first\_value = last row logically

***

## 11️⃣ Handling NULL Values (IMPORTANT)

### Default Behavior

`first_value` **does NOT skip nulls** by default.

```python
data = [
    ("A", 1, None),
    ("A", 2, 100),
]
```

```python
first_value("sales").over(w)
```

❌ Result → `NULL`

***

### ✅ Correct Way (Spark 3.x+)

```python
first_value("sales", ignorenulls=True).over(w)
```

✅ Output → `100`

***

## 12️⃣ `first_value` vs `row_number`

| Use case                      | Better function |
| ----------------------------- | --------------- |
| Tag all rows with first value | first\_value    |
| Keep only one row             | row\_number     |

Example with row\_number:

```python
from pyspark.sql.functions import row_number

df.withColumn(
    "rn", row_number().over(w)
).filter("rn = 1")
```

***

## 13️⃣ Real‑World Use Cases

✅ First transaction per customer  
✅ First login date  
✅ Opening balance  
✅ First salary per employee  
✅ Initial state in event streams  
✅ Baseline metrics  
✅ Slowly changing dimensions (SCD)

***

## 14️⃣ Performance Notes

*   Window functions cause **shuffle**
*   Partition keys should be **well-distributed**
*   Avoid very large partitions
*   Combine window functions where possible

***

## 15️⃣ Common Interview Traps

❌ Forgetting window frame  
❌ Using `last_value` incorrectly  
❌ Using `min()` instead of `first_value`  
❌ Ignoring null behavior  
❌ Expecting groupBy-style collapse

***

## ✅ Interview‑Perfect Explanation (Say This)

> “`first_value` is a window function that returns the first value in an ordered window partition. It respects ordering and does not reduce row count. I explicitly define the window frame to ensure correctness and use DESC order when I need last‑known values.”

***

## ✅ One‑Line Memory Aids

✅ *“Order matters for first\_value.”*  
✅ *“Window functions don’t collapse rows.”*  
✅ *“Define frames explicitly.”*  
✅ *“DESC + first\_value = latest record.”*

***
Just tell me 👍
