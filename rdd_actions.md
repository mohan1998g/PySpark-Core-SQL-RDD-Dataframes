Excellent — below is **everything you asked for**, delivered in a **single, well‑structured, interview‑ready master guide** covering:

1.  ✅ **Complete list of RDD ACTIONS (with definitions & examples)**
2.  ✅ **Transformation vs Action → DAG & execution explanation**
3.  ✅ **One‑page‑style cheat sheet (text version)**
4.  ✅ **Mock PySpark RDD interview (advanced level, with answers)**

***

# ✅ 1. COMPLETE LIST OF PYSPARK RDD ACTIONS

*(Actions trigger execution of the DAG and return results to the driver or external storage)*

***

## 1. `collect()`

**Definition:** Returns all elements of an RDD to the driver.

```python
rdd.collect()
```

⚠️ **Dangerous for large data**

***

## 2. `count()`

Returns number of elements.

```python
rdd.count()
```

***

## 3. `first()`

Returns first element.

```python
rdd.first()
```

***

## 4. `take(n)`

Returns first `n` elements.

```python
rdd.take(3)
```

***

## 5. `takeOrdered(n, key=None)`

Returns smallest `n` elements.

```python
rdd.takeOrdered(3)
```

***

## 6. `top(n)`

Returns largest `n` elements.

```python
rdd.top(2)
```

***

## 7. `reduce(func)`

Aggregates values using associative function.

```python
rdd.reduce(lambda a,b: a+b)
```

***

## 8. `fold(zeroValue, func)`

Like reduce, but with initial value.

```python
rdd.fold(0, lambda a,b: a+b)
```

***

## 9. `aggregate(zero, seqOp, combOp)`

Advanced aggregation.

```python
rdd.aggregate(0, lambda acc,x: acc+x, lambda a,b: a+b)
```

***

## 10. `foreach(func)`

Executes function on each element (executors).

```python
rdd.foreach(lambda x: print(x))
```

***

## 11. `foreachPartition(func)`

Runs once per partition.

***

## 12. `countByValue()`

Counts occurrences of each value.

```python
rdd.countByValue()
```

***

## 13. `countByKey()`

Counts values per key.

```python
pairs.countByKey()
```

***

## 14. `lookup(key)`

Returns values for a key.

```python
pairs.lookup(1)
```

***

## 15. `saveAsTextFile(path)`

Writes RDD to storage.

***

## ✅ Action Summary (Interview Line)

> “RDD actions trigger execution and either return results to the driver or write them externally.”

***
