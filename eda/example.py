from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import when, col, explode, split

access_token = '<access-token>'
try:
    spark = SparkSession.builder.remote(f"<spark-connect-endpoint>").getOrCreate()
    print("Spark is running. Version:", spark.version)
except Exception as e:
    print("Spark is not running:", e)

# Define dataset
data = [
    Row(tweet_id="t001", user_id="user1", tweet_text="I love Spark!", hashtags="spark"),
    Row(tweet_id="t002", user_id="user2", tweet_text="PySpark is amazing!", hashtags="pyspark"),
    Row(tweet_id="t003", user_id="user3", tweet_text="Bad day with Spark errors.", hashtags="spark"),
    Row(tweet_id="t004", user_id="user1", tweet_text="Happy to learn Spark!", hashtags="spark,learning"),
    Row(tweet_id="t005", user_id="user4", tweet_text="I hate bugs in code!", hashtags="code,bugs"),
]

# Convert to Spark DataFrame
df = spark.createDataFrame(data)

# Display the DataFrame
print("Initial Data:")
df.show()

# Step 1: Hashtag Analysis - Count occurrences of each hashtag
# Split hashtags by comma, then explode into individual rows
hashtags_df = df.withColumn("hashtag", explode(split(col("hashtags"), ",")))

# Group by hashtags and count occurrences
hashtag_counts = hashtags_df.groupBy("hashtag").count().orderBy("count", ascending=False)
print("Hashtag Counts:")
hashtag_counts.show()

# Step 2: Sentiment Analysis - Simple sentiment based on keywords
# We'll label tweets with positive or negative sentiment based on certain keywords
positive_keywords = ["love", "amazing", "happy"]
negative_keywords = ["bad", "hate", "errors"]

# Add a sentiment column based on the presence of keywords
df = df.withColumn(
    "sentiment",
    when(
        col("tweet_text").rlike("|".join(positive_keywords)), "positive"
    ).when(
        col("tweet_text").rlike("|".join(negative_keywords)), "negative"
    ).otherwise("neutral")
)

print("Data with Sentiment Analysis:")
df.select("tweet_id", "tweet_text", "sentiment").show()

# Step 3: User Activity Analysis - Count tweets per user
user_activity = df.groupBy("user_id").count().withColumnRenamed("count", "tweet_count").orderBy("tweet_count", ascending=False)
print("User Activity (Tweet Count by User):")
user_activity.show()