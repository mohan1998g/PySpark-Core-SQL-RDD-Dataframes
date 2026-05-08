Below is a **complete, structured theory** of **lambda functions in PySpark RDDs**, followed by **25 clear, practical examples** (well beyond the minimum 20), covering almost every common RDD transformation and action.

***

# Lambda Functions in PySpark RDDs

## 1. Introduction

In **PySpark**, **lambda functions** are widely used with **RDD (Resilient Distributed Dataset)** operations to define small, anonymous functions inline. They allow you to express transformation logic **concisely** without defining full functions using `def`.

RDDs are the **low-level distributed data abstraction** in Spark, and most RDD transformations (`map`, `filter`, `reduce`, etc.) expect a **function** as an argument. Lambda functions fit perfectly here.

***

## 2. What is a Lambda Function?

A **lambda function** in Python is:

*   An **anonymous function**
*   Defined using the `lambda` keyword
*   Can have **any number of parameters**
*   Must contain **only a single expression**
*   Returns the result of that expression automatically

### Syntax

```python
lambda arguments: expression
```

### Example

```python
lambda x: x + 1
```

Equivalent normal function:

```python
def add_one(x):
    return x + 1
```

***

## 3. Why Use Lambda Functions in PySpark RDDs?

### Key Reasons:

1.  **Concise syntax** (less boilerplate code)
2.  **Readable inline logic**
3.  Ideal for **simple transformations**
4.  Common pattern in Spark APIs
5.  Less mental overhead for short operations

***

## 4. Where Lambda Functions Are Used in RDDs

Lambda functions are commonly used in:

### Transformations

*   `map()`
*   `filter()`
*   `flatMap()`
*   `mapValues()`
*   `keyBy()`
*   `reduceByKey()`
*   `groupByKey()`
*   `sortBy()`
*   `distinct()`

### Actions

*   `reduce()`
*   `countByValue()`
*   `foreach()`
*   `takeOrdered()`

***

## 5. Key Rules & Limitations

| Rule                        | Explanation             |
| --------------------------- | ----------------------- |
| Single expression only      | No multiple statements  |
| No assignments              | `x = x + 1` not allowed |
| Limited debugging           | Harder to debug errors  |
| Not ideal for complex logic | Use `def` instead       |

***

## 6. Lambda Functions and Spark Execution

*   Lambda functions are **serialized** and sent to worker nodes
*   They execute **in parallel**
*   Must not reference **non-serializable objects**
*   Should avoid heavy external dependencies

***

# 7. 25 Practical Examples of Lambda Functions in PySpark RDDs

Assume SparkContext is already created:

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])
```
Below are the **exact outputs** for **each of the 25 lambda-based PySpark RDD examples** I shared earlier.  
Assumptions:

*   Spark runs locally
*   Order-sensitive outputs are shown as Spark typically returns them
*   `collect()` is used unless otherwise stated

***

# ✅ Outputs for All 25 Lambda Function Examples in PySpark RDDs

***

## Example 1: Square numbers

```python
rdd.map(lambda x: x * x).collect()
```

**Output**

```text
[1, 4, 9, 16, 25]
```

***

## Example 2: Convert int to string

```python
rdd.map(lambda x: str(x)).collect()
```

**Output**

```text
['1', '2', '3', '4', '5']
```

***

## Example 3: Filter even numbers

```python
rdd.filter(lambda x: x % 2 == 0).collect()
```

**Output**

```text
[2, 4]
```

***

## Example 4: Numbers greater than 3

```python
rdd.filter(lambda x: x > 3).collect()
```

**Output**

```text
[4, 5]
```

***

## Example 5: flatMap – split words

```python
text.flatMap(lambda x: x.split(" ")).collect()
```

**Output**

```text
['hello', 'world', 'pyspark', 'lambda']
```

***

## Example 6: reduce – sum of elements

```python
rdd.reduce(lambda a, b: a + b)
```

**Output**

```text
15
```

***

## Example 7: reduce – maximum value

```python
rdd.reduce(lambda a, b: a if a > b else b)
```

**Output**

```text
5
```

***

## Example 8: map + filter

```python
rdd.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
```

**Output**

```text
[6, 8, 10]
```

***

## Example 9: Create key-value pairs

```python
rdd.map(lambda x: (x, x * 10)).collect()
```

**Output**

```text
[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
```

***

## Example 10: reduceByKey – sum

```python
pairs.reduceByKey(lambda a, b: a + b).collect()
```

**Input**

```python
[(1,2), (1,3), (2,4)]
```

**Output**

```text
[(1, 5), (2, 4)]
```

***

## Example 11: mapValues

```python
pairs.mapValues(lambda x: x + 1).collect()
```

**Output**

```text
[(1, 3), (1, 4), (2, 5)]
```

***

## Example 12: filter on key-value RDD

```python
pairs.filter(lambda x: x[0] == 1).collect()
```

**Output**

```text
[(1, 2), (1, 3)]
```

***

## Example 13: groupByKey

```python
pairs.groupByKey().mapValues(lambda x: list(x)).collect()
```

**Output**

```text
[(1, [2, 3]), (2, [4])]
```

***

## Example 14: sortBy – ascending

```python
rdd.sortBy(lambda x: x).collect()
```

**Output**

```text
[1, 2, 3, 4, 5]
```

***

## Example 15: sortBy – descending

```python
rdd.sortBy(lambda x: x, ascending=False).collect()
```

**Output**

```text
[5, 4, 3, 2, 1]
```

***

## Example 16: distinct

```python
sc.parallelize([1,1,2,2,3]).distinct().collect()
```

**Output**

```text
[1, 2, 3]
```

***

## Example 17: keyBy

```python
rdd.keyBy(lambda x: x % 2).collect()
```

**Output**

```text
[(1, 1), (0, 2), (1, 3), (0, 4), (1, 5)]
```

***

## Example 18: countByValue

```python
sc.parallelize(["a", "b", "a"]).countByValue()
```

**Output**

```text
{'a': 2, 'b': 1}
```

***

## Example 19: takeOrdered – smallest 3

```python
rdd.takeOrdered(3, lambda x: x)
```

**Output**

```text
[1, 2, 3]
```

***

## Example 20: foreach (prints on executors)

```python
rdd.foreach(lambda x: print(x))
```

**Output (printed)**

```text
1
2
3
4
5
```

⚠️ Order is **not guaranteed** in cluster mode.

***

## Example 21: mapPartitions

```python
rdd.mapPartitions(lambda it: (x * 2 for x in it)).collect()
```

**Output**

```text
[2, 4, 6, 8, 10]
```

***

## Example 22: filter strings by length

```python
words.filter(lambda x: len(x) > 3).collect()
```

**Input**

```python
["spark", "py", "lambda"]
```

**Output**

```text
['spark', 'lambda']
```

***

## Example 23: Transform tuple values

```python
pairs.map(lambda x: (x[0], x[1] * 2)).collect()
```

**Output**

```text
[(1, 4), (1, 6), (2, 8)]
```

***

## Example 24: Word count

```python
text.flatMap(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
```

**Output**

```text
[('hello', 1), ('world', 1), ('pyspark', 1), ('lambda', 1)]
```

***

## Example 25: Remove None values

```python
sc.parallelize([1, None, 2, None, 3]) \
  .filter(lambda x: x is not None).collect()
```

**Output**

```text
[1, 2, 3]
```

***

## ✅ Final Notes (Interview Tip)

*   **RDD outputs often have no guaranteed order** unless sorted
*   Lambda logic runs **on worker nodes**
*   `foreach()` prints on executors, not always driver
*   Mastering these patterns covers **90% of RDD interview questions**

## 8. When NOT to Use Lambda Functions

❌ Complex logic  
❌ Multiple branching conditions  
❌ Extensive error handling  
❌ Large reusable logic

✅ Use `def` functions instead for clarity and maintainability.

***

## 9. Summary

*   Lambda functions are **core to PySpark RDD programming**
*   Best suited for **small, stateless logic**
*   Widely used in transformations and actions
*   Increase readability and reduce boilerplate
*   Should be avoided for complex operations

***

If you want, I can also provide:
✅ **Interview questions**
✅ **Comparison: lambda vs def in PySpark**
✅ **Real-world RDD use cases**
✅ **Practice problems with solutions**

Just tell me 👍
