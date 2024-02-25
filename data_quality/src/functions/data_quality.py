from lib_unit_tests_dq.functions.parse_yaml import ParseYaml
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType

class SparkDataQuality:
    def __init__(self, spark, schema_registry_path):
        self.spark = spark
        self.schema_registry_path = schema_registry_path
        self.exception = None
        self.dict_types = {
            "string": StringType(),
            "integer": IntegerType(),
            "float": FloatType(),
            "double": DoubleType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }

    def get_spark_type(self,yaml_type):
        return self.dict_types.get(yaml_type.lower())
        
    def validate_schema(self, df, table_name):
        schema = ParseYaml.parse(f"{self.schema_registry_path}{table_name}.yaml")
        for col_info in schema[table_name]:
            col_name = col_info['column']
            col_type = self.get_spark_type(col_info['type'])
            if col_name not in df.columns:
                raise ValueError(f"Column {col_name} not found in DataFrame, expected {col_name} to be present in DataFrame")
            if col_type != df.schema[col_name].dataType:
                raise ValueError(f"Column {col_name} has wrong type, expected {col_type} but got {df.schema[col_name].dataType}")
        return True
    
    def validate_nulls(self, df, table_name):
        schema = ParseYaml.parse(f"{self.schema_registry_path}{table_name}.yaml")
        for col_info in schema[table_name]:
            col_name = col_info['column']
            if col_info['nullable'] == False:
                if df.filter(col(col_name).isNull()).count() > 0:
                    raise ValueError(f"Column {col_name} has null values, expected no null values in {col_name}")
        return True