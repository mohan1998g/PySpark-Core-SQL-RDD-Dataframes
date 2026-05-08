Let’s explain this **clearly, step‑by‑step**, exactly the way TCS (and similar companies) expect you to explain it in interviews.

***

## ✅ Code to Explain

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

rdd.keyBy(lambda x: x % 2) \
   .mapValues(lambda x: 1) \
   .reduceByKey(lambda a, b: a + b) \
   .collect()
```

***

## ✅ Step 1: Initial RDD

```python
[1, 2, 3, 4, 5]
```

***

## ✅ Step 2: `keyBy(lambda x: x % 2)`

### What `keyBy` does

*   Converts a **normal RDD** into a **key‑value RDD**
*   The lambda generates the **key**
*   The original value becomes the **value**

### Applying `x % 2`

*   `0` → even numbers
*   `1` → odd numbers

### Result after `keyBy`

| x | x % 2 | Pair   |
| - | ----- | ------ |
| 1 | 1     | (1, 1) |
| 2 | 0     | (0, 2) |
| 3 | 1     | (1, 3) |
| 4 | 0     | (0, 4) |
| 5 | 1     | (1, 5) |

✅ RDD now becomes:

```text
[(1,1), (0,2), (1,3), (0,4), (1,5)]
```

***

## ✅ Step 3: `mapValues(lambda x: 1)`

### What `mapValues` does

*   Operates **only on values**
*   Keeps keys unchanged

### Transformation

Every value is converted to `1`

✅ Result:

```text
[(1,1), (0,1), (1,1), (0,1), (1,1)]
```

This means:

*   Each number contributes **count = 1**
*   We are preparing for counting

***

## ✅ Step 4: `reduceByKey(lambda a, b: a + b)`

### What `reduceByKey` does

*   Groups values by key
*   Aggregates them using the given function
*   Performs **map‑side aggregation** (efficient)

### Reduction

*   Key `1` (odd): `1 + 1 + 1 = 3`
*   Key `0` (even): `1 + 1 = 2`

✅ Result:

```text
[(1, 3), (0, 2)]
```

***

## ✅ Step 5: `collect()`

*   Brings final result to the **driver**
*   Triggers Spark execution

***

## ✅ ✅ Final Output

```text
[(1, 3), (0, 2)]
```

***

## ✅ What This Code Achieves (In One Line)

👉 **Counts odd and even numbers in the RDD**

*   `1 → 3` → three odd numbers
*   `0 → 2` → two even numbers

***

## ✅ Interview‑Perfect Explanation (Say This)

> “First, I convert the RDD into a key‑value RDD using `keyBy` where the key identifies odd or even numbers. Then I assign a count of 1 to each value using `mapValues`. Finally, I use `reduceByKey` to aggregate counts per key, resulting in the number of odd and even elements.”

***

## ✅ Why This Is a GOOD Spark Solution

✅ Uses `reduceByKey` instead of `groupByKey`  
✅ Avoids unnecessary shuffles  
✅ Scales well for large data  
✅ Very common real‑world pattern (classification + counting)

***
