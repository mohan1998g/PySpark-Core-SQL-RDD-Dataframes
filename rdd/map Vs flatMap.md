Let’s evaluate it step by step 👇

```python
rdd = sc.parallelize(["hello world", "spark rdd"])
rdd.map(lambda x: x.split()).collect()
```

***

## ✅ What the code does

*   `rdd` contains **two strings**
*   `map()` applies the lambda to **each element**
*   `x.split()` splits each string into a **list of words**
*   `map` does **NOT flatten** the result

***

## ✅ Element‑by‑Element Transformation

| Input Element   | `x.split()` Result   |
| --------------- | -------------------- |
| `"hello world"` | `['hello', 'world']` |
| `"spark rdd"`   | `['spark', 'rdd']`   |

***

## ✅ Final Output

```text
[['hello', 'world'], ['spark', 'rdd']]
```

***

## 🚨 Important Interview Insight

If you **wanted a flat list**, `map` is ❌ wrong — you must use `flatMap` ✅

### Correct way to flatten:

```python
rdd.flatMap(lambda x: x.split()).collect()
```

✅ Output:

```text
['hello', 'world', 'spark', 'rdd']
```

***

## ✅ Key Takeaway (Say This in Interviews)

> "`map` returns one output per input, while `flatMap` can return multiple outputs and flattens them."

Just let me know 👍
