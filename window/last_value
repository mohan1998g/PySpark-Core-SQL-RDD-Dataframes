Below is a **complete, interview‑ready explanation of the `last_value` window function in PySpark**, covering **theory, syntax, default behavior, the BIG trap, correct usage, examples, edge cases, and interview tips**.

This is one of the **most commonly misunderstood window functions** — interviewers *love* asking it.

***

# ✅ `last_value` Window Function in PySpark — Complete Theory & Examples

***

## 1️⃣ What is `last_value`?

### ✅ Definition

`last_value` is a **window function** that returns the **last value in an ordered window frame** for each row.

> ⚠️ The key words are **“window frame”** — *not* just “partition”.

***

## 2️⃣ Why `last_value` Is Tricky (BIG INTERVIEW TRAP 🚨)

When you use `ORDER BY` in a window specification, Spark **changes the default frame**.

### Default Frame (when ORDER BY is present)

```text
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

✅ This means:

*   The frame **stops at the current row**
*   So `last_value` becomes the **current row’s value**
*   ❌ NOT the actual last value in the partition

👉 **Most people get this wrong**.

***

## 3️⃣ Syntax

```python
from pyspark.sql.functions import last_value
from pyspark.sql.window import Window
```

```python
last_value(column).over(window_spec)
```

***

## 4️⃣ Example Dataset

```python
data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-02", 120),
    ("A", "2024-01-03", 110),
    ("B", "2024-01-01", 200),
    ("B", "2024-01-02", 220),
]

df = spark.createDataFrame(data, ["dept", "date", "sales"])
```

***

## 5️⃣ WRONG Usage (Most Common Mistake ❌)

### Window Spec (missing frame)

```python
w = Window.partitionBy("dept").orderBy("date")
```

### Code

```python
df.withColumn(
    "last_sales_wrong",
    last_value("sales").over(w)
).show()
```

### ❌ Output (WRONG for most use cases)

```text
+----+----------+-----+-----------------+
|dept|date      |sales|last_sales_wrong |
+----+----------+-----+-----------------+
|A   |2024-01-01|100  |100              |
|A   |2024-01-02|120  |120              |
|A   |2024-01-03|110  |110              |
|B   |2024-01-01|200  |200              |
|B   |2024-01-02|220  |220              |
+----+----------+-----+-----------------+
```

### ❓ Why is this wrong?

Because the window frame ends at **CURRENT ROW**, so Spark thinks:

> “The last value so far is just this row”.

***

## 6️⃣ ✅ CORRECT Usage (Always Do This)

### ✅ Explicit Window Frame (BEST PRACTICE)

```python
w = (
    Window
    .partitionBy("dept")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)
```

### Code

```python
df.withColumn(
    "last_sales",
    last_value("sales").over(w)
).show()
```

### ✅ Correct Output

```text
+----+----------+-----+-----------+
|dept|date      |sales|last_sales |
+----+----------+-----+-----------+
|A   |2024-01-01|100  |110        |
|A   |2024-01-02|120  |110        |
|A   |2024-01-03|110  |110        |
|B   |2024-01-01|200  |220        |
|B   |2024-01-02|220  |220        |
+----+----------+-----+-----------+
```

✅ Now every row sees the **true last value of the partition**.

***

## 7️⃣ Alternative Trick (Descending Order + first\_value)

Many teams prefer this for safety:

```python
w = Window.partitionBy("dept").orderBy(df.date.desc())
```

```python
df.withColumn(
    "last_sales",
    first_value("sales").over(w)
).show()
```

✅ Also correct  
✅ Avoids frame confusion  
✅ Very common in production

***

## 8️⃣ Handling NULL Values (IMPORTANT)

### Default Behavior

`last_value` **does NOT ignore NULLs**.

```text
Last value = NULL (if last row is NULL)
```

***

### ✅ Ignore NULLs (Spark 3+)

```python
last_value("sales", ignorenulls=True).over(w)
```

✅ This skips nulls and returns the **last non‑null value**.

***

## 9️⃣ `last_value` vs `max` (INTERVIEW TRAP)

| Aspect                | last\_value | max  |
| --------------------- | ----------- | ---- |
| Respects ordering     | ✅ Yes       | ❌ No |
| Needs ORDER BY        | ✅ Yes       | ❌ No |
| Chronological meaning | ✅ Yes       | ❌ No |

👉 `max()` gives biggest number, **not the latest value**.

***

## 🔥 10️⃣ Real‑World Use Cases

✅ Closing balance per account  
✅ Last transaction per user  
✅ Latest status per order  
✅ Current salary per employee  
✅ Slowly Changing Dimensions (SCD Type‑2)  
✅ Event stream latest state

***

## 11️⃣ Performance Notes

*   Causes **shuffle**
*   Large partitions hurt performance
*   Choose correct partition keys
*   Combine multiple window functions if possible

***

## 12️⃣ Common Interview Questions on `last_value`

### ❓ Why does `last_value` often give wrong results?

✅ Because the default window frame ends at the current row.

***

### ❓ How do you fix it?

✅ Define:

```python
rowsBetween(unboundedPreceding, unboundedFollowing)
```

or use `ORDER BY DESC + first_value`.

***

## ✅ Interview‑Perfect Explanation (Say This)

> “`last_value` returns the last value in the window frame, not just the partition. Because Spark’s default frame ends at the current row when ORDER BY is used, I explicitly define the frame or use DESC order with first\_value to ensure correctness.”

***

## ✅ One‑Line Memory Aids

✅ *“Frame matters more than function.”*  
✅ *“last\_value without frame is almost always wrong.”*  
✅ *“DESC + first\_value = safest latest value.”*

***

## ✅ Summary Table

| Requirement      | Recommended               |
| ---------------- | ------------------------- |
| Latest value     | `first_value` + DESC      |
| True last value  | `last_value` + full frame |
| Ignore NULLs     | `ignorenulls=True`        |
| Interview safety | Always explain frame      |

***
