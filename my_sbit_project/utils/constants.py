from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, DataType, \
    LongType, TimestampType, MapType, DateType


address_struct = StructType([
    StructField("street_address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True),
])

