from time import time

# Measure execution time for Spark job
start_time = time()

# Your Spark job code here

end_time = time()
print(f"Spark Execution Time: {end_time - start_time} seconds")
