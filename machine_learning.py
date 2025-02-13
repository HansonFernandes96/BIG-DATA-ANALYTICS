from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SentimentAnalysis").getOrCreate()

# Load and preprocess the dataset
df = spark.read.csv("s3://Cloud-Based Big Data Analytics with Apache Spark and Hadoop/amazon-reviews.csv", header=True, inferSchema=True)

# Index labels (sentiments)
indexer = StringIndexer(inputCol="sentiment", outputCol="label")
df = indexer.fit(df).transform(df)

# Assemble features
assembler = VectorAssembler(inputCols=["review_length", "helpful_votes"], outputCol="features")
df = assembler.transform(df)

# Train Logistic Regression model
lr = LogisticRegression(maxIter=10, regParam=0.01)
model = lr.fit(df)

# Make predictions
predictions = model.transform(df)
predictions.show()

# Evaluate the model (accuracy, precision, recall)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy: {accuracy}")
