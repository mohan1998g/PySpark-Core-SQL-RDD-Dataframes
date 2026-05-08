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

***

## Example 1: map – Square numbers

```python
rdd.map(lambda x: x * x).collect()
```

***

## Example 2: map – Convert int to string

```python
rdd.map(lambda x: str(x)).collect()
```

***

## Example 3: filter – Even numbers

```python
rdd.filter(lambda x: x % 2 == 0).collect()
```

***

## Example 4: filter – Numbers greater than 3

```python
rdd.filter(lambda x: x > 3).collect()
```

***

## Example 5: flatMap – Split words

```python
text = sc.parallelize(["hello world", "pyspark lambda"])
text.flatMap(lambda x: x.split(" ")).collect()
```

***

## Example 6: reduce – Sum of elements

```python
rdd.reduce(lambda a, b: a + b)
```

***

## Example 7: reduce – Maximum value

```python
rdd.reduce(lambda a, b: a if a > b else b)
```

***

## Example 8: map + filter together

```python
rdd.map(lambda x: x * 2).filter(lambda x: x > 5).collect()
```

***

## Example 9: key-value RDD using map

```python
rdd.map(lambda x: (x, x * 10)).collect()
```

***

## Example 10: reduceByKey – Sum values

```python
pairs = sc.parallelize([(1, 2), (1, 3), (2, 4)])
pairs.reduceByKey(lambda a, b: a + b).collect()
```

***

## Example 11: mapValues – Increment values

```python
pairs.mapValues(lambda x: x + 1).collect()
```

***

## Example 12: filter on key-value RDD

```python
pairs.filter(lambda x: x[0] == 1).collect()
```

***

## Example 13: groupByKey

```python
pairs.groupByKey().mapValues(lambda x: list(x)).collect()
```

***

## Example 14: sortBy – Ascending

```python
rdd.sortBy(lambda x: x).collect()
```

***

## Example 15: sortBy – Descending

```python
rdd.sortBy(lambda x: x, ascending=False).collect()
```

***

## Example 16: distinct

```python
sc.parallelize([1, 1, 2, 2, 3]).distinct().collect()
```

*(No lambda needed, but common in pipelines)*

***

## Example 17: keyBy – Create key-value pairs

```python
rdd.keyBy(lambda x: x % 2).collect()
```

***

## Example 18: countByValue

```python
sc.parallelize(["a", "b", "a"]).countByValue()
```

***

## Example 19: takeOrdered – Smallest 3

```python
rdd.takeOrdered(3, lambda x: x)
```

***

## Example 20: foreach – Print elements

```python
rdd.foreach(lambda x: print(x))
```

***

## Example 21: mapPartitions

```python
rdd.mapPartitions(lambda it: (x * 2 for x in it)).collect()
```

***

## Example 22: filter strings by length

```python
words = sc.parallelize(["spark", "py", "lambda"])
words.filter(lambda x: len(x) > 3).collect()
```

***

## Example 23: Transform tuple values

```python
pairs.map(lambda x: (x[0], x[1] * 2)).collect()
```

***

## Example 24: Word count using lambdas

```python
text.flatMap(lambda x: x.split()) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .collect()
```

***

## Example 25: Remove None values

```python
sc.parallelize([1, None, 2, None, 3]) \
  .filter(lambda x: x is not None) \
  .collect()
```

***

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
