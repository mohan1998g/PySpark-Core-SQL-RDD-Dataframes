Below is a **complete, exam‑ready list of PySpark RDD transformations**, with **clear definitions and simple examples**. This covers **all commonly used RDD transformations you’re expected to know** for interviews and real usage.

> ✅ **Note:**
>
> *   **Transformations are lazy** (they don’t execute until an action is called)
> *   They always return a **new RDD**

***

# ✅ Complete List of PySpark RDD Transformations

***

## 1. `map(func)`

**Definition:**  
Applies a function to **each element** of the RDD and returns a new RDD.

```python
rdd.map(lambda x: x * 2).collect()
```

**Output**

```text
[2, 4, 6, 8, 10]
```

***

## 2. `flatMap(func)`

**Definition:**  
Like `map`, but **flattens the output** (1 input → 0 or many outputs).

```python
rdd = sc.parallelize(["hello world", "spark rdd"])
rdd.flatMap(lambda x: x.split()).collect()
```

**Output**

```text
['hello', 'world', 'spark', 'rdd']
```

***

## 3. `filter(func)`

**Definition:**  
Returns only elements that satisfy the condition.

```python
rdd.filter(lambda x: x % 2 == 0).collect()
```

**Output**

```text
[2, 4]
```

***

## 4. `mapPartitions(func)`

**Definition:**  
Applies a function to **each partition** instead of each element.

```python
rdd.mapPartitions(lambda it: (x * 2 for x in it)).collect()
```

**Output**

```text
[2, 4, 6, 8, 10]
```

***

## 5. `mapPartitionsWithIndex(func)`

**Definition:**  
Like `mapPartitions`, but also provides **partition index**.

```python
rdd.mapPartitionsWithIndex(lambda i, it: [(i, list(it))]).collect()
```

**Output**

```text
[(0, [1,2,3,4,5])]
```

***

## 6. `distinct()`

**Definition:**  
Removes duplicate elements.

```python
sc.parallelize([1,1,2,2,3]).distinct().collect()
```

**Output**

```text
[1, 2, 3]
```

***

## 7. `union(otherRDD)`

**Definition:**  
Combines two RDDs (duplicates allowed).

```python
rdd1.union(rdd2).collect()
```

***

## 8. `intersection(otherRDD)`

**Definition:**  
Returns common elements between RDDs.

```python
rdd1.intersection(rdd2).collect()
```

***

## 9. `subtract(otherRDD)`

**Definition:**  
Removes elements present in another RDD.

```python
rdd1.subtract(rdd2).collect()
```

***

## 10. `cartesian(otherRDD)`

**Definition:**  
Returns **all possible pairs** (⚠️ very expensive).

```python
rdd1.cartesian(rdd2).collect()
```

***

# 🔑 Key‑Value RDD Transformations

***

## 11. `keyBy(func)`

**Definition:**  
Creates `(key, value)` pairs using a function.

```python
rdd.keyBy(lambda x: x % 2).collect()
```

**Output**

```text
[(1,1), (0,2), (1,3), (0,4), (1,5)]
```

***

## 12. `mapValues(func)`

**Definition:**  
Applies function **only to values**, keeps keys same.

```python
pairs.mapValues(lambda x: x + 1).collect()
```

***

## 13. `flatMapValues(func)`

**Definition:**  
Like `mapValues`, but flattens the output.

```python
pairs.flatMapValues(lambda x: [x, x*2]).collect()
```

***

## 14. `reduceByKey(func)`

**Definition:**  
Aggregates values **per key** using a function (✅ preferred).

```python
pairs.reduceByKey(lambda a,b: a+b).collect()
```

***

## 15. `groupByKey()`

**Definition:**  
Groups values by key (⚠️ expensive shuffle).

```python
pairs.groupByKey().mapValues(list).collect()
```

***

## 16. `combineByKey(...)`

**Definition:**  
Custom aggregation (used internally by `reduceByKey`).

```python
pairs.combineByKey(
    lambda x: (x,1),
    lambda acc,x: (acc[0]+x, acc[1]+1),
    lambda acc1,acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1])
).collect()
```

***

## 17. `aggregateByKey(zero, seqFunc, combFunc)`

**Definition:**  
Aggregation with separate logic for **within** and **across** partitions.

***

## 18. `sortByKey()`

**Definition:**  
Sorts data by key.

```python
pairs.sortByKey().collect()
```

***

## 19. `sortBy(func, ascending=True)`

**Definition:**  
Sorts by computed value.

```python
rdd.sortBy(lambda x: -x).collect()
```

***

## 20. `join(otherRDD)`

**Definition:**  
Inner join on keys.

```python
rdd1.join(rdd2).collect()
```

***

## 21. `leftOuterJoin(otherRDD)`

```python
rdd1.leftOuterJoin(rdd2).collect()
```

***

## 22. `rightOuterJoin(otherRDD)`

***

## 23. `fullOuterJoin(otherRDD)`

***

## 24. `cogroup(otherRDD)`

**Definition:**  
Groups values from multiple RDDs by key.

```python
rdd1.cogroup(rdd2).collect()
```

***

# 📦 Partition & Structure Transformations

***

## 25. `repartition(numPartitions)`

**Definition:**  
Increases or decreases partitions (full shuffle).

***

## 26. `coalesce(numPartitions)`

**Definition:**  
Reduces partitions without full shuffle.

***

## 27. `zip(otherRDD)`

**Definition:**  
Pairs elements from two RDDs (same partition & size required).

***

## 28. `zipWithIndex()`

```python
rdd.zipWithIndex().collect()
```

***

## 29. `zipWithUniqueId()`

***

## 30. `sample(withReplacement, fraction, seed)`

```python
rdd.sample(False, 0.5).collect()
```

***

## 31. `randomSplit(weights, seed)`

***

# ✅ Narrow vs Wide Transformations (INTERVIEW GOLD)

| Type   | Examples                                         |
| ------ | ------------------------------------------------ |
| Narrow | `map`, `filter`, `flatMap`, `mapValues`          |
| Wide   | `reduceByKey`, `groupByKey`, `sortByKey`, `join` |

***

# ✅ One‑Line Interview Summary

> “RDD transformations are lazy operations that create new RDDs. Narrow transformations don’t require shuffles, while wide transformations do, which impacts performance.”

***

If you want next:
✅ **Complete list of RDD actions**  
✅ **Transformation vs action DAG explanation**  
✅ **Cheat sheet PDF format**  
✅ **Mock Spark interview (advanced)**

Just tell me 🚀
