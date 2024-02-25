from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from lib_unit_tests_dq.functions.data_quality import SparkDataQuality

spark = SparkSession.builder.appName("test").getOrCreate()
schema_registry_path = "./example/schema_registry/"
dq = SparkDataQuality(spark, schema_registry_path)

def test_validate_schema():
    data = spark.read.csv("./example/data/people.csv", header=True)
    assert dq.validate_schema(data, "people") == True
    
def validate_nulls():
    data = spark.read.csv("./example/data/people.csv", header=True)
    assert dq.validate_nulls(data, "people") == True

if __name__ == "__main__":
    test_validate_schema()