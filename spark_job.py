from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("AmazonReviewAnalysis").getOrCreate()

# Load the dataset from S3 (or local)
df = spark.read.json("s3://amazon-reviews-2023.json")

# Example: Aggregating review counts by product
product_reviews = df.groupBy("product_id").count()
product_reviews.show()

# Save the results if needed

product_reviews.write.csv("s3:/amazon_review.csv")
