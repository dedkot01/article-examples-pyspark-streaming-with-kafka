from pyspark.sql.types import IntegerType, StringType, StructField, StructType

schema = StructType(
    [
        StructField('name', StringType(), True),
        StructField('age', IntegerType(), True),
    ],
)
