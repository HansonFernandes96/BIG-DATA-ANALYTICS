
### **mapper.py** (Hadoop MapReduce Mapper)
```python
import sys

for line in sys.stdin:
    words = line.strip().split()
    for word in words:
        print(f"{word}\t1")
