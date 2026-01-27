In **PySpark**, both `rank()` and `dense_rank()` are window functions used to assign ordering numbers to rows **within a window partition** — but they behave differently **when there are ties**.

Here’s the simple difference:

***

# ✅ **Difference Between `rank()` and `dense_rank()` in PySpark**

## **1. `rank()`**

*   Assigns the **same rank** to tied values.
*   **Skips subsequent ranks** after a tie (creates gaps).

### Example:

Values: `10, 20, 20, 30`

| Value | rank()                                            |
| ----- | ------------------------------------------------- |
| 10    | 1                                                 |
| 20    | 2                                                 |
| 20    | 2                                                 |
| 30    | 4  ← (rank 3 is skipped because of the tie above) |

***

## **2. `dense_rank()`**

*   Assigns the **same rank** to tied values.
*   **Does NOT skip** the next rank (no gaps).

### Example:

Values: `10, 20, 20, 30`

| Value | dense\_rank() |
| ----- | ------------- |
| 10    | 1             |
| 20    | 2             |
| 20    | 2             |
| 30    | 3  ← (no gap) |

***

# 🧪 PySpark Example Code

```python
from pyspark.sql.window import Window
import pyspark.sql.functions as F

df = spark.createDataFrame(
    [(1, 10), (2, 20), (3, 20), (4, 30)],
    ["id", "value"]
)

windowSpec = Window.orderBy("value")

df.withColumn("rank", F.rank().over(windowSpec)) \
  .withColumn("dense_rank", F.dense_rank().over(windowSpec)) \
  .show()
```

Output:

    +---+-----+----+----------+
    | id|value|rank|dense_rank|
    +---+-----+----+----------+
    |  1|   10|   1|         1|
    |  2|   20|   2|         2|
    |  3|   20|   2|         2|
    |  4|   30|   4|         3|
    +---+-----+----+----------+

***

# 🎯 **Quick Summary**

| Function       | Ties?              | Gaps in ranking?     | Use case                                            |
| -------------- | ------------------ | -------------------- | --------------------------------------------------- |
| `rank()`       | Same rank for ties | **Yes**, gaps appear | When you want “competition style” ranking (1,2,2,4) |
| `dense_rank()` | Same rank for ties | **No gaps**          | When you want sequential ranking (1,2,2,3)          |

***

If you want, I can also compare this with `row_number()` or show real-world examples like “Top N per group”.
