# Example Notebook for Spark Connect Usage
# Step 1: Ensure that all Python dependencies listed in the 'requirements.txt' file are installed.
#         Refer to the 'README.md' file for detailed installation instructions.
# Step 2: Obtain the connection details for the remote Spark Connect Clusters from the IOMETE console.
#         Then, incorporate these details into the Spark builder configuration in this notebook.


from pyspark.sql import SparkSession
from pyspark.sql import Row
from datetime import datetime, date

spark = SparkSession.builder.remote("sc://...").getOrCreate()

print("> Example dataframe operations")
df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df.show()

print("> Query tables from the remote cluster")
spark.sql("show tables").show()
spark.sql("select * from default.employees limit 10").show()
