# Spark SQL & DataFrame Operations (With Outputs)

## Sample Data
DataFrame df:
| id | tdate | amount | category | product | spendby |
|----|--------|--------|-----------|-----------|----------|
|0|06-26-2011|300.4|Exercise|GymnasticsPro|cash|
|1|05-26-2011|200.0|Exercise Band|Weightlifting|credit|
|2|06-01-2011|300.4|Exercise|Gymnastics Pro|cash|

---

## 1. Select All
```sql
SELECT * FROM df;
```

**Output (sample):**
```
0,06-26-2011,300.4,Exercise,GymnasticsPro,cash
1,05-26-2011,200.0,Exercise Band,Weightlifting,credit
```

---

## 2. Order By
```sql
SELECT id,tdate FROM df ORDER BY id;
```

**Output:**
```
0 06-26-2011
1 05-26-2011
```

---

## 3. Filter Condition
```sql
SELECT * FROM df WHERE category='Exercise';
```

**Output:**
```
0 Exercise
2 Exercise
6 Exercise
```

---

## 4. Aggregation
```sql
SELECT MAX(id) FROM df;
```

**Output:**
```
8
```

---

## 5. CASE WHEN
```sql
SELECT spendby, CASE WHEN spendby='cash' THEN 1 ELSE 0 END AS status FROM df;
```

**Output:**
```
cash 1
credit 0
```

---

## 6. CONCAT
```sql
SELECT CONCAT(id,'-',category) FROM df;
```

**Output:**
```
0-Exercise
1-Exercise Band
```

---

## 7. GROUP BY
```sql
SELECT category, SUM(amount) FROM df GROUP BY category;
```

**Output (sample):**
```
Exercise 700.8
Gymnastics 400.0
```

---

## 8. WINDOW FUNCTION
```sql
SELECT category, amount,
ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC)
FROM df;
```

**Output:**
```
Exercise 300.4 1
Exercise 300.4 2
Exercise 100.0 3
```

---

## 9. JOINS
```sql
SELECT a.id,a.name,b.product FROM cust a JOIN prod b ON a.id=b.id;
```

**Output:**
```
1 raj mouse
3 sai mobile
```

---

## 10. DATE CONVERSION
```sql
SELECT FROM_UNIXTIME(UNIX_TIMESTAMP(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') FROM df;
```

**Output:**
```
2011-06-26
2011-05-26
```

---

## Notes
- Outputs are representative (may vary slightly)
- Focus on logic for interviews
