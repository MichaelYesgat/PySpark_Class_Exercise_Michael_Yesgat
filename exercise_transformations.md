# Exercise: Transformations

## Overview
Apply various RDD transformations to solve data processing problems. Understand narrow vs wide transformations.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Core Tasks

### Task 1: Narrow Transformations

Create `transformations.py`:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "Transformations")

# Sample log data
logs = sc.parallelize([
    "2024-01-15 10:00:00 INFO User login: alice",
    "2024-01-15 10:01:00 ERROR Database connection failed",
    "2024-01-15 10:02:00 INFO User login: bob",
    "2024-01-15 10:03:00 WARN Memory usage high",
    "2024-01-15 10:04:00 ERROR Timeout occurred",
    "2024-01-15 10:05:00 INFO Data processed: 1000 records",
    "2024-01-15 10:06:00 DEBUG Cache hit rate: 95%"
])

# Task A: Filter only ERROR logs
errors = # YOUR CODE
print(f"Errors: {errors.collect()}")

# Task B: Extract just the log level from each line
# Expected: ["INFO", "ERROR", "INFO", "WARN", "ERROR", "INFO", "DEBUG"]
levels = # YOUR CODE (hint: split and take index 2)
print(f"Levels: {levels.collect()}")

# Task C: Chain - get messages from ERROR logs only
# Expected: ["Database connection failed", "Timeout occurred"]
error_messages = # YOUR CODE
print(f"Error messages: {error_messages.collect()}")
```

### Task 2: Wide Transformations

```python
# Sample word data
words = sc.parallelize([
    "spark", "hadoop", "spark", "data", "hadoop", 
    "spark", "python", "data", "spark", "scala"
])

# Task A: distinct() - Get unique words
unique = # YOUR CODE
print(f"Unique words: {unique.collect()}")

# Task B: Group and count (wide transformation)
word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
print(f"Word counts: {word_counts.collect()}")

# Task C: sortBy - Sort by count descending
sorted_counts = # YOUR CODE (sort by count, descending)
print(f"Sorted: {sorted_counts.collect()}")
```

### Task 3: Set Operations

```python
# Two datasets
set1 = sc.parallelize([1, 2, 3, 4, 5])
set2 = sc.parallelize([4, 5, 6, 7, 8])

# Task A: union() - Combine both sets
combined = # YOUR CODE
print(f"Union: {combined.collect()}")

# Task B: intersection() - Common elements
common = # YOUR CODE
print(f"Intersection: {common.collect()}")

# Task C: subtract() - Elements in set1 but not in set2
difference = # YOUR CODE
print(f"Difference: {difference.collect()}")
```

### Task 4: Practical Pipeline

Build a log analysis pipeline:

```python
# Given: Web server logs
web_logs = sc.parallelize([
    "192.168.1.1 GET /home 200 150ms",
    "192.168.1.2 GET /products 200 230ms",
    "192.168.1.1 POST /login 200 180ms",
    "192.168.1.3 GET /home 404 50ms",
    "192.168.1.2 GET /products 200 210ms",
    "192.168.1.1 GET /home 200 120ms",
    "192.168.1.4 GET /admin 403 30ms"
])

# Build a pipeline to:
# 1. Filter only successful requests (status 200)
# 2. Extract the endpoint (e.g., /home)
# 3. Count requests per endpoint
# 4. Sort by count descending

# YOUR PIPELINE CODE HERE
# Expected output: [('/products', 2), ('/home', 2), ('/login', 1)]
```

---

## Deliverables

1. `transformations.py` - Complete script
2. Output showing all transformation results

---

## Definition of Done

- [ ] Narrow transformations (filter, map) work correctly
- [ ] Wide transformations (distinct, reduceByKey, sortBy) work
- [ ] Set operations produce correct results
- [ ] Log analysis pipeline produces expected output
- [ ] Understand which transformations cause shuffles

---

## Additional Resources
- Written Content: `transformations.md`
